#!/usr/bin/env python3 
# encoding: utf-8
'''
s3tools.ManageSnapshots -- shortdesc

s3tools.ManageSnapshots is a description

It defines classes_and_methods

@copyright:  2020 Collins Aerospace. All rights reserved.

@license:    BSD 3-Clause License
'''

import boto3
import datetime
import logging
import os
import re
import sys
import xml.etree.ElementTree as ElementTree

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from botocore.exceptions import ClientError
from gi.importer import repository
from posixpath import join as urljoin
from posixpath import sep as urlpathsep
from pprint import pformat

import P2CompositeUtils
import S3Utils


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

PRODUCT_ASSET_PATTERN = re.compile(r'com.collins.fmw.ide-\d+\.\d+\.\d+-(\d{12})-.*')


def manage_snapshots(bucket_name, bucket_prefix, retain_days=30, retain_minimum=3, retain_maximum=20, child_regex=None):
    try:
        session = boto3.Session()
        s3_resource = session.resource('s3')
        s3_client = session.client('s3')
        bucket = s3_resource.Bucket(bucket_name)
        tree = P2CompositeUtils.get_composite_artifacts_xml(s3_client, bucket.name, bucket_prefix)
        root = tree.getroot()
        children_element = P2CompositeUtils.get_children_element(root)
        child_keys = {}
        for child in children_element.getchildren():
            child_location = child.get('location')
            print('Checking child location %s' % (child_location))
            if child_location is None:
                # Malformed child, delete from the tree
                children_element.remove(child)
            else:
                # Skip those that are URLs
                if P2CompositeUtils.is_location_url(child_location):
                    print('Skipping URL child location %s' % (child_location))
                else:
                    # Check that the child may be found and get its last modification time
                    last_modified = None
                    # TODO: what if the child is also composite?  Skip it?  Recurse into it?
                    # For now we'll call this YAGNI as we don't intend to manage composites of
                    # composites with this tool.
                    for bkey in S3Utils.get_matching_s3_contents(bucket.name, urljoin(bucket_prefix, child_location), 'p2.index'):
                        last_modified = bkey['LastModified']
                        print('Found child %s modified %s' % (child_location, last_modified))
                    if last_modified is None:
                        # The child could not be found, remove it from the tree 
                        # TODO: should we attempt to delete all sub-objects at the prefix corresponding to the location?
                        children_element.remove(child)
                    else:
                        child_keys[child_location] = last_modified
        print('Child keys: %s' % (pformat(child_keys)))
        if child_regex is not None:
            pattern = re.compile(child_regex)
            child_keys = {ck: clm for ck, clm in child_keys.items() if pattern.fullmatch(ck) is not None}
        print('Filtered child keys: %s' % (pformat(child_keys)))
        sorted_child_keys = sorted(child_keys.items(), reverse=True, key=lambda x: x[1])
        print('Sorted child keys: %s' % (pformat(sorted_child_keys)))
        # filter to obtain the keys to delete
        retained_count = 0
        current_datetime = datetime.datetime.now()
        retain_after = None if retain_days is None else (current_datetime - datetime.timedelta(days=retain_days))
        delete_keys = []
        for key in sorted_child_keys:
            if retain_minimum is not None and retained_count < retain_minimum:
                retained_count = retained_count + 1
            elif retain_maximum is not None and retain_maximum <= retained_count:
                delete_keys.append(key)
            elif retain_after is not None:
                if key[1] < retain_after:
                    delete_keys.append(key)
                else:
                    retained_count = retained_count + 1
        print('Deleting children: %s' % (pformat(delete_keys)))
        for child in delete_keys:
            print('Deleting child %s at %s' % (child[0], urljoin(bucket_prefix, child[0])))
            #P2CompositeUtils.remove_repository_from_composite(bucket_name, bucket_prefix, urljoin(bucket_prefix, child[0]))
    except ClientError as e:
        logging.error(e)
        return False
    return True

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
        parser.add_argument("-v", "--verbose",
            dest="verbose", action="count", help="set verbosity level [default: %(default)s]")
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
        parser.add_argument('--age', dest="age",
            help="the age in days to retain child repositories [zero indicates forever, default: %(default)]",
            metavar="min",
            default='30')
        parser.add_argument('--min', dest="min",
            help="the minumum number of child repositories to retain [default: %(default)]",
            metavar="min",
            default='3')
        parser.add_argument('--max', dest="max",
            help="the maximum number of child repositories to retain [default: %(default)]",
            metavar="max",
            default='20')
        parser.add_argument('--name-regex', dest="name_regex",
            help="regular expression to searchchild names for each path [default: %(default)]",
            metavar="name_regex",
            default=None)

        # Process arguments
        args = parser.parse_args()

        bucket_name = args.bucket
        bucket_prefix = args.prefix
        retain_days = int(args.age)
        retain_minimum = int(args.min)
        retain_maximum = int(args.max)
        child_regex = args.name_regex

        verbose = args.verbose

        if verbose > 0:
            print("Verbose mode on")
            print(program_version_message)
            print('AWS Bucket: %s' % (bucket_name))
            print('Key prexix: %s' % (bucket_prefix))
            print('Retaining for %d days.' % (retain_days))
            print('Retaining between %d minimum and %d maximum children.' % (retain_minimum, retain_maximum))
            print('Child search regex: %s' % (child_regex))

        cred = boto3.session.Session().get_credentials()
        if cred is None:
            print('Please provide Boto3 credentials.')
            sys.exit(-1)

        manage_snapshots(bucket_name, bucket_prefix, retain_days, retain_minimum, retain_maximum, child_regex)

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
        sys.argv.append("-v")
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
