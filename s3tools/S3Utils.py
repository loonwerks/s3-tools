#!/usr/bin/env python3 
# encoding: utf-8
'''
s3tools.S3Utils -- shortdesc

s3tools.S3Utils is a description

@copyright:  2020 Collins Aerospace. All rights reserved.

@license:    BSD 3-Clause License
'''

import boto3
import logging
import os
import pathlib
import tempfile

from botocore.exceptions import ClientError
from posixpath import join as urljoin
from pprint import pformat

logger = logging.getLogger('s3tools.S3Utils')

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
        logger.debug("get common prefixes query result: %s" % pformat(query_result))
        common_prefixes = [o.get('Prefix') for o in query_result.get('CommonPrefixes')]
    except ClientError as e:
        logger.error(e)
        raise
    return common_prefixes

def get_matching_s3_contents(bucket, prefix='', suffix=''):
    """
    Generate the contents in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        try:
            # The S3 API response is a large blob of metadata.
            # 'Contents' contains information about the listed objects.
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp['Contents']:
                key = obj['Key']
                if key.startswith(prefix) and key.endswith(suffix):
                    yield obj

            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
        
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
        logger.info('Uploading file {} to bucket {} key {}'.format(file_name, bucket.name, object_name))
        s3_client.upload_file(file_name, bucket.name, object_name, ExtraArgs={'ServerSideEncryption': 'AES256', 'ACL': 'public-read'})
    except ClientError as e:
        logger.error(e)
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
        s3_client.put_object(Body=body, Bucket=bucket, Key=object_name, ExtraArgs={'ServerSideEncryption': 'AES256', 'ACL': 'public-read'})
        logger.info('Uploading string body to {} to {}'.format(object_name, bucket.name))
    except ClientError as e:
        logger.error(e)
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
        logger.error(e)
        return False
    return True

def remove_repository(s3_client, bucket, prefix):
    """Remove a P2 repository from a prefix on an S3 bucket

    :param s3_client Boto s3_client object to apply
    :param bucket: Bucket to remove from
    :param prefix: Prefix at which to remove the repository
    :return: True if repository was removed, else False
    """
    try:
        logger.info("Removing repository tree in bucket %s at %s" % (bucket.name, prefix))
        bucket.objects.filter(Prefix=prefix).delete()
    except ClientError as e:
        logger.error(e)
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
    logger.debug('Uploading file_obj contents: %s' % (file.read().decode()))
    file.seek(0)
    logger.debug('Uploading file_obj to key %s in bucket %s' % (key, bucket))
    try:
       s3_client.upload_fileobj(file, bucket, key, ExtraArgs={'ServerSideEncryption': 'AES256', 'ACL': 'public-read'})
    except ClientError as e:
        logger.error("Response: %s" % (pformat(e.response)))
        logger.error(e)
        raise e
