#!/usr/bin/env python3 
# encoding: utf-8
'''
PublishHandler -- AWS Lambda handler for publishing into a composite P2 repository hosted on S3

@copyright:  2021 Collins Aerospace. All rights reserved.

@license:    BSD 3-Clause License
'''

import codepipeline
import os
import json
import logging

from s3tools.P2CompositeUtils import add_repository_to_composite

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def put_job_success(job_id):
    """Notify AWS CodePipeline of a successful job.

    Arguments:
        job_id {str} -- The unique ID for the job generated by AWS CodePipeline
    """
    logger.info('Putting job success')
    codepipeline.put_job_success_result(
        jobId=job_id,
        executionDetails={
            'summary': "Success",
            'percentComplete': 100
        }
    )
    
def put_job_failure(job_id, message):
    """Notify AWS CodePipeline of a successful job.

    Arguments:
        job_id {str} -- The unique ID for the job generated by AWS CodePipeline
    """
    logger.info('Putting job failure result=%s', message)
    codepipeline.put_job_success_result(
        jobId=job_id,
        executionDetails={
            'summary': "Failure: " + str(message),
            'percentComplete': 100
        }
    )
    
def lambda_handler(event, context):
    logger.info('Event: %s', event)
    logger.info('Context: %s', context)
    job_id = event["CodePipeline.job"].id
    if isinstance(event, dict):
        try:
            add_repository_to_composite(event['inpath'], event['bucket_name'], event['bucket_prefix'], event['child_name'])
            message = f'Published {event['inpath']} to {event['bucket_name']}/{event['bucket_prefix']}/{event['child_name']}'
            put_job_success(job_id)
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
            put_job_failure(job_id, message)
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
        put_job_failure(job_id, message)
        logger.error('%s, returning 422 response', message)
        return {
            "statusCode": 422,
            "headers": {
                "Content-Type": "application/text"
            },
            "body": message
        }