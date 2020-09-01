#!/usr/bin/env python3 
# encoding: utf-8
'''
s3tools.DeployToP2CompositeRepository -- shortdesc

s3tools.DeployToP2CompositeRepository is a description

It defines classes_and_methods

@copyright:  2020 Collins Aerospace. All rights reserved.

@license:    BSD 3-Clause License
'''

import boto3
import logging
import os
import P2CompositeUtils
import pathlib
import sys
import tempfile
import time
import xml.etree.ElementTree as ElementTree

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from botocore.exceptions import ClientError
from cupshelpers.config import prefix
from gi.importer import repository
from mako.template import Template
from posixpath import join as urljoin


__all__ = []
__version__ = 0.1
__date__ = '2020-07-21'
__updated__ = '2020-07-21'

DEBUG = 1
TESTRUN = 0
PROFILE = 0

DEFAULT_SITE = '''http://ca-trustedsystems-dev-us-east-1.s3-website-us-east-1.amazonaws.com/'''
DEFAULT_BUCKET_NAME = '''ca-trustedsystems-dev-us-east-1'''

COMPOSITE_ARTIFACTS_TEMPLATE = Template(
'''<?xml version='1.0' encoding='UTF-8'?>
<?compositeArtifactRepository version='1.0.0'?>
<repository name='&quot;${name}&quot;'
    type='org.eclipse.equinox.internal.p2.artifact.repository.CompositeArtifactRepository' version='1.0.0'>
  <properties size='1'>
    <property name='p2.timestamp' value='${timestamp}'/>
  </properties>
  <children size='${len(contents)}'>
% for element in contents:
    <child location='${element}'/>
% endfor
  </children>
</repository>
''')

COMPOSITE_CONTENT_TEMPLATE = Template(
'''<?xml version='1.0' encoding='UTF-8'?>
<?compositeMetadataRepository version='1.0.0'?>
<repository name='&quot;${name}&quot;'
    type='org.eclipse.equinox.internal.p2.metadata.repository.CompositeMetadataRepository' version='1.0.0'>
  <properties size='1'>
    <property name='p2.timestamp' value='${timestamp}'/>
  </properties>
  <children size='${len(contents)}'>
% for element in contents:
    <child location='${element}'/>
% endfor
  </children>
</repository>
''')

def get_common_prefixes(bucket, prefix):
    """Get the prefixes common to the given prefix in the given bucket
    
    :param bucket: Bucket to query for common prefixes
    :param prefix: string prefix at which query
    :return: List containing the common prefixes
    """
    query_result = bucket.meta.client.list_objects(Bucket=bucket.name,
                                                   Prefix=prefix,
                                                   Delimiter='/')
    try:
        common_prefixes = [o.get('PrefiX') for o in query_result.get('CommonPrefixes')]
    except ClientError as e:
        logging.error(e)
        raise
    return common_prefixes

def upload_file(file_name, s3_client, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: String path to file to upload
    :param s3_client Boto s3_client object to apply
    :param bucket: Boto Bucket object to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        #s3_client.upload_file(file_name, bucket, object_name, ExtraArgs={'ACL': 'public-read'})
        print('Uploading file {} to bucket {} key {}'.format(file_name, bucket.name, object_name))
    except ClientError as e:
        logging.error(e)
        raise
    return True

def upload_string(body, s3_client, bucket, object_name):
    """Upload a string as to the contents of an S3 bucket

    :param body: String contents to be placed in the named object
    :param s3_client Boto s3_client object to apply
    :param bucket: Boto Bucket object to upload to
    :param object_name: S3 object name
    :return: True if file was uploaded, else False
    """
    try:
        s3_client.put_object(Body=body, Bucket=bucket, Key=object_name, ExtraArgs={'ACL': 'public-read'})
        print('Uploading string body to {} to {}'.format(object_name, bucket.name))
    except ClientError as e:
        logging.error(e)
        raise
    return True

def upload_repository(repository, s3_client, bucket, prefix):
    """Upload a P2 repository to a prefix on an S3 bucket

    :param repository: Path to the repository to upload
    :param s3_client Boto s3_client object to apply
    :param bucket: Bucket to upload to
    :param prefix: Prefix at which to upload the repository
    :return: True if repository was uploaded, else False
    """
    try:
        for dir_name, _, file_list in os.walk(repository):
            for file_name in file_list:
                file_path = os.path.join(dir_name, file_name)
                upload_file(file_path, s3_client, bucket, 
                            urljoin(prefix, pathlib.PurePosixPath(os.path.relpath(file_path, repository))))
    except ClientError as e:
        logging.error(e)
        return False
    return True

def get_spooled_file_object(s3_client, bucket, key):
    """Get a temporary spooled file object for an S3 object

    :param s3_client Boto s3_client object to apply
    :param bucket: Bucket to upload to
    :param key: key identifying the object within the bucket
    """
    result = tempfile.SpooledTemporaryFile()
    s3_client.download_fileobj(bucket, key, result)
    result.seek(0)
    return result

def upload_file_object(s3_client, bucket, key, file):
    """Upload a file object for an S3 object

    :param s3_client Boto s3_client object to apply
    :param bucket: Bucket to upload to
    :param key: key identifying the object within the bucket
    :param file: the file object to be uploaded
    """
    file.seek(0)
    s3_client.upload_fileobj(file, bucket, key)

def update_composite_artifacts(s3_client, bucket, repo_prefix, child_location, new_timestamp=None):
    """Update the P2 compositeArtifacts.xml
    """
    composite_artifacts_key = urljoin(repo_prefix, 'compositeArtifacts.xml')
    file_obj = tempfile.SpooledTemporaryFile()
    try:
        s3_client.download_fileobj(bucket, composite_artifacts_key, file_obj)
    except ClientError as e:
        if e.response.hasKey('ResponseMetadata') and e.response['ResponseMetadata'].hasKey('HTTPStatusCode') and e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            logging.info('{key} not found, generating initial composite artifacts'.format(key=composite_artifacts_key))
            file_obj.seek(0)
            file_obj.truncate()
            file_obj.write(P2CompositeUtils.EMPTY_COMPOSITE_ARTIFACTS.encode('utf-8'))
        else:
            logging.error(e)
            raise e
    file_obj.seek(0)
    tree = ElementTree.parse(file_obj)
    root = tree.getroot()
    children = P2CompositeUtils.get_children(root)
    P2CompositeUtils.add_child(root, child_location)
    children.attrib['size'] = len(children)
    P2CompositeUtils.update_timestamp(root, new_timestamp)
    try:
        upload_file_object(s3_client, bucket, composite_artifacts_key, file_obj)
        os.close(file_obj)
    except ClientError as e:
        logging.error(e)
        os.close(file_obj)
        raise e

def add_repository_to_composite(repository, bucket_name, prefix, new_child):
    """Upload a P2 repository to a prefix containing a composite repository on an S3 bucket

    :param repository: Path to the repository to upload
    :param bucket: Bucket to upload to
    :param prefix: Prefix at which the composite repository is found
    :param new_child: the extension to the prefix where the child is to be located
    :return: True if repository was uploaded, else False
    """
    try:
        session = boto3.Session(profile_name='AWSFed-comm-dev')
        s3_resource = session.resource('s3')
        s3_client = session.client('s3')
        bucket = s3_resource.Bucket(bucket_name)
        existing_children = get_common_prefixes(bucket, prefix)
        upload_repository(repository, s3_client, bucket, urljoin(prefix, new_child))
        #children = existing_children + [new_child]
        timestamp = int(round(time.time() * 1000.0))
        update_composite_artifacts(s3_client, bucket.name, prefix, new_child, timestamp)
        #upload_string(COMPOSITE_ARTIFACTS_TEMPLATE.render(name='Composite P2', contents=children, timestamp=timestamp), s3_client, bucket, urljoin(prefix, 'compositeArtifacts.xml'))
        #upload_string(COMPOSITE_CONTENT_TEMPLATE.render(name='Composite P2', contents=children, timestamp=timestamp), s3_client, bucket, urljoin(prefix, 'compositeContents.xml'))
    except ClientError as e:
        logging.error(e)
        return False
    return True

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

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
        parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]")
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument(dest="paths", help="paths to folder(s) with source file(s) [default: %(default)s]", metavar="path", nargs='+')

        # Process arguments
        args = parser.parse_args()

        paths = args.paths
        verbose = args.verbose

        if verbose > 0:
            print("Verbose mode on")

        for inpath in paths:
            ### do something with inpath ###
            print(inpath)

        add_repository_to_composite('/home/kfhoech/git/AGREE-Updates/agree_2.5.1', DEFAULT_BUCKET_NAME, 'p2', 'agree_2.5.1')

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