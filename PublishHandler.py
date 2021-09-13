#!/usr/bin/env python3 
# encoding: utf-8
'''
PublishHandler -- AWS Lambda handler for publishing into a composite P2 repository hosted on S3

@copyright:  2021 Collins Aerospace. All rights reserved.

@license:    BSD 3-Clause License
'''

import os
import json
import logging

from s3tools.P2CompositeUtils import add_repository_to_composite

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info('Event: %s', event)
    logger.info('Context: %s', context)
    if isinstance(event, dict):
        try:
            add_repository_to_composite(event['inpath'], event['bucket_name'], event['bucket_prefix'], event['child_name'])
            message = f'Published {event['inpath']} to {event['bucket_name']}/{event['bucket_prefix']}/{event['child_name']}'
            logger.info('%s, returning 200 OK respnse', message)
            return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/text"
            },
            "body": message
            }
        except Exception as e:
            message = f'Exception occurred {repr(e)}'
            logger.error('%s, returning 417 response', message)
            return {
            "statusCode": 417,
            "headers": {
                "Content-Type": "application/text"
            },
            "body": message
            }
    else:
        message = f'Unexpected event type {type(event)}'
        logger.error('%s, returning 422 response', message)
        return {
            "statusCode": 422,
            "headers": {
                "Content-Type": "application/text"
            },
            "body": message
        }