#!/usr/bin/env python3 
# encoding: utf-8
'''
s3tools.RemoveChildFromP2CompositeRepository -- shortdesc

s3tools.RemoveChildFromP2CompositeRepository is a description

It defines classes_and_methods

@copyright:  2020 Collins Aerospace. All rights reserved.

@license:    BSD 3-Clause License
'''

import boto3
import logging
import os
import sys
import xml.etree.ElementTree as ElementTree

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from gi.importer import repository
from posixpath import join as urljoin
from pprint import pformat

import P2CompositeUtils
import S3Utils

logger = logging.getLogger('s3tools')

__all__ = []
__version__ = 0.1
__date__ = '2020-07-21'
__updated__ = '2020-07-21'

DEBUG = 1
TESTRUN = 0
PROFILE = 0

DEFAULT_SITE = '''http://ca-trustedsystems-dev-us-east-1.s3-website-us-east-1.amazonaws.com/'''
DEFAULT_BUCKET_NAME = '''ca-trustedsystems-dev-us-east-1'''
DEFAULT_BUCKET_PREFIX = '''p2'''


def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Copyright 2020 Collins Aerospace. All rights reserved.

  Licensed under the BSD 3-Clause License
  http://raw.githubusercontent.com/loonwerks/s3-tools/master/LICENSE

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc)

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("--logging", dest="loglevel",
            help="set logging verbosity level, one of CRITICAL, ERROR, WARNING, INFO, or DEBUG [default: %(default)s]",
            metavar="loglevel",
            default='INFO')
        parser.add_argument('-V', '--version',
            action='version', version=program_version_message)
        parser.add_argument('--bucket', dest="bucket",
            help="the AWS bucket in which the P2 composite repositories are stored [default: %(default)]",
            metavar="bucket",
            default=DEFAULT_BUCKET_NAME)
        parser.add_argument('--prefix', dest="prefix",
            help="the key prefix in the AWS bucket from which to remove the child repositories [default: %(default)]",
            metavar="prefix",
            default=DEFAULT_BUCKET_PREFIX)
        parser.add_argument('--child-name', dest="child_name",
            action='append',
            help="child names for each path [default: is the last segment of the corresponding path]",
            metavar="child_name",
            nargs='+')

        # Process arguments
        args = parser.parse_args()

        bucket_name = args.bucket
        bucket_prefix = args.prefix
        child_names = [item for sublist in ([] if args.child_name is None else args.child_name) for item in sublist]

        # assuming loglevel is bound to the string value obtained from the
        # command line argument. Convert to upper case to allow the user to
        # specify --logging=DEBUG or --logging=debug
        numeric_level = getattr(logging, args.loglevel.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % args.loglevel)
        logging.basicConfig()
        logger.setLevel(numeric_level)

        logger.info(program_version_message)
        logger.info("Child names: %s" % (pformat(child_names)))

        cred = boto3.session.Session().get_credentials()
        if cred is None:
            sys.stderr.write('No AWS credentials. Please provide Boto3 credentials.\n')
            sys.exit(-1)

        for child_name in child_names:
            logger.debug('remove_repository_from_composite(%s, %s, %s)' % (bucket_name, bucket_prefix, child_name))
            P2CompositeUtils.remove_repository_from_composite(bucket_name, bucket_prefix, child_name)

        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception as e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == "__main__":
    if DEBUG:
        sys.argv.append("--logging=DEBUG")
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = 's3tools.DeployToP2CompositeRepository_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())
