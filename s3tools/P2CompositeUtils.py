#!/usr/bin/env python3 
# encoding: utf-8
'''
s3tools.P2CompositeUtils -- shortdesc

s3tools.P2CompositeUtils is a description

To be used with xml.etree.elementtree, handles reading, writing, and updating
the contents of an Eclipse P2 composite repository compositeArtifacts.xml and
compositeContent.xml specification.

@copyright:  2020 Collins Aerospace. All rights reserved.

@license:    BSD 3-Clause License
'''

import boto3
import logging
import tempfile
import time

import xml.etree.ElementTree as ElementTree

from botocore.exceptions import ClientError
from mako.template import Template
from posixpath import join as urljoin

import S3Utils

logger = logging.getLogger('s3tools.P2CompositeUtils')

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

EMPTY_COMPOSITE_ARTIFACTS_XML = '''<?xml version='1.0' encoding='UTF-8'?>
<?compositeArtifactRepository version='1.0.0'?>
<repository name='&quot;Eclipse Project Test Site&quot;'
    type='org.eclipse.equinox.internal.p2.artifact.repository.CompositeArtifactRepository' version='1.0.0'>
  <properties size='1'>
    <property name='p2.timestamp' value='0000000000000'/>
  </properties>
  <children size='0'>
  </children>
</repository>
'''

EMPTY_COMPOSITE_CONTENT_XML = '''<?xml version='1.0' encoding='UTF-8'?>
<?compositeMetadataRepository version='1.0.0'?>
<repository name='&quot;Eclipse Project Test Site&quot;'
    type='org.eclipse.equinox.internal.p2.metadata.repository.CompositeMetadataRepository' version='1.0.0'>
  <properties size='1'>
    <property name='p2.timestamp' value='0000000000000'/>
  </properties>
  <children size='0'>
  </children>
</repository>
'''

@deprecated(reason="Nasty")
def build_empty_composite_artifacts():
    repository = ElementTree.Element('repository')
    repository['name'] = '&quot;Eclipse Project Test Site&quot;'
    repository['type'] = 'org.eclipse.equinox.internal.p2.artifact.repository.CompositeArtifactRepository'
    repository['version'] = '1.0.0'
    properties = ElementTree.SubElement(repository, 'properties')
    timestamp_property = ElementTree.SubElement(properties, 'property')
    timestamp_property['name'] = 'p2.timestamp'
    timestamp_property['value'] = '0'
    properties['size'] = '1'
    children = ElementTree.SubElement(repository, 'children')
    children['size'] = '0'
    return repository


def is_location_url(x):
    """Check whether the 
    """
    from urllib.parse import urlparse
    try:
        result = urlparse(x)
        return all([result.scheme in ['http', 'https'], result.netloc, result.path])
    except:
        return False

def get_properties(element_tree_root):
    properties = element_tree_root.findall('properties')
    if len(properties) < 1:
        return ElementTree.SubElement(element_tree_root, 'properties', {'size' : '0'})
    return properties[0]

def get_children_element(element_tree_root):
    children = element_tree_root.findall('children')
    if len(children) < 1:
        return ElementTree.SubElement(element_tree_root, 'children', {'size' : '0'})
    return children[0]

def get_timestamp_property(element_tree_root):
    result = []
    properties = get_properties(element_tree_root)
    timestamps = [p for p in properties.findall('property') if 'name' in p.attrib and p.attrib['name'] == 'p2.timestamp']
    if len(timestamps) < 1:
        result.append(ElementTree.SubElement(properties, 'property', {'name' : 'p2.timestamp', 'value' : '0000000000000'}))
    else:
        result.extend(timestamps)
    return result

def update_timestamp(element_tree_root, timestamp=None):
    timestamp = str(int(round(time.time() * 1000.0))) if timestamp is None else timestamp
    for props in get_timestamp_property(element_tree_root):
        props.set('value', str(timestamp))

def get_child_locations(element_tree_root):
    return [child.attrib['location'] for child in get_children_element(element_tree_root).findall('child') if 'location' in child.attrib]

def add_child(element_tree_root, location):
    children = get_children_element(element_tree_root)
    ElementTree.SubElement(children, 'child', {'location' : location})
    children.set('size', str(len(children)))

def remove_child(element_tree_root, location):
    children = get_children_element(element_tree_root)
    for child_element in children:
        child_location = child_element.get('location')
        if child_location is None or child_location == location:
            children.remove(child_element)
    children.set('size', str(len(children)))

def write_to_string(element_tree_root):
    return ElementTree.tostring(element_tree_root, encoding='UTF-8', method='xml').decode()

def read_from_string(xml_str):
    return ElementTree.fromstring(xml_str)

def synch_compostite_artifacts_to_composite_content(s3_client, bucket_name, repo_prefix, composite_artifacts_tree):
    composite_artifacts_root = composite_artifacts_tree.getroot()

    file_obj = tempfile.TemporaryFile()
    file_obj.write(EMPTY_COMPOSITE_CONTENT_XML.encode('utf-8'))
    file_obj.truncate()
    file_obj.seek(0)
    composite_content_tree = ElementTree.parse(file_obj)
    composite_content_root = composite_content_tree.getroot()
    file_obj.seek(0)

    for child in get_children_element(composite_artifacts_root):
        add_child(composite_content_root, child.get('location'))
    for timestamp_property in get_timestamp_property(composite_artifacts_root):
        update_timestamp(composite_content_root, timestamp_property.get('value'))

    store_composite_content_xml(s3_client, bucket_name, repo_prefix, composite_content_tree)

def get_composite_artifacts_xml(s3_client, bucket, repo_prefix):
    composite_artifacts_key = urljoin(repo_prefix, 'compositeArtifacts.xml')
    file_obj = tempfile.TemporaryFile()
    try:
        logger.debug('Downloading object at key %s from bucket %s' % (composite_artifacts_key, bucket))
        s3_client.download_fileobj(bucket, composite_artifacts_key, file_obj)
    except ClientError as e:
        if 'ResponseMetadata' in e.response and 'HTTPStatusCode' in e.response['ResponseMetadata'] and e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            logger.info('{key} not found, generating initial composite artifacts'.format(key=composite_artifacts_key))
            file_obj.seek(0)
            file_obj.write(EMPTY_COMPOSITE_ARTIFACTS_XML.encode('utf-8'))
            file_obj.truncate()
        else:
            logger.error(e)
            file_obj.close()
            raise e
    file_obj.seek(0)
    tree = ElementTree.parse(file_obj)
    file_obj.seek(0)
    logger.debug("Retrieved composite artifacts contents: %s" % (file_obj.read().decode()))
    file_obj.close()
    return tree

def store_composite_artifacts_xml(s3_client, bucket_name, repo_prefix, tree):
    composite_artifacts_key = urljoin(repo_prefix, 'compositeArtifacts.xml')
    file_obj = tempfile.TemporaryFile()
    file_obj.seek(0)
    file_obj.write(ElementTree.tostring(tree.getroot(), encoding='utf-8'))
    file_obj.truncate()
    try:
        logger.info("Uploading composite artifacts to bucket %s at %s" % (bucket_name, composite_artifacts_key))
        S3Utils.upload_file_object(s3_client, bucket_name, composite_artifacts_key, file_obj)
        file_obj.close()
    except ClientError as e:
        logger.error(e)
        file_obj.close()
        raise e

def store_composite_content_xml(s3_client, bucket_name, repo_prefix, tree):
    composite_content_key = urljoin(repo_prefix, 'compositeContent.xml')
    file_obj = tempfile.TemporaryFile()
    file_obj.seek(0)
    file_obj.write(ElementTree.tostring(tree.getroot(), encoding='utf-8'))
    file_obj.truncate()
    try:
        logger.info("Uploading composite content to bucket %s at %s" % (bucket_name, composite_content_key))
        S3Utils.upload_file_object(s3_client, bucket_name, composite_content_key, file_obj)
        file_obj.close()
    except ClientError as e:
        logger.error(e)
        file_obj.close()
        raise e

def add_child_to_composite_artifacts(s3_client, bucket_name, repo_prefix, child_location, new_timestamp=None):
    """Update the P2 compositeArtifacts.xml
    """
    tree = get_composite_artifacts_xml(s3_client, bucket_name, repo_prefix)
    root = tree.getroot()
    children = get_children_element(root)
    add_child(root, child_location)
    children.attrib['size'] = str(len(children))
    update_timestamp(root, new_timestamp)
    store_composite_artifacts_xml(s3_client, bucket_name, repo_prefix, tree)
    synch_compostite_artifacts_to_composite_content(s3_client, bucket_name, repo_prefix, tree)

def remove_child_from_composite_artifacts(s3_client, bucket_name, repo_prefix, child_location, new_timestamp=None):
    """Update the P2 compositeArtifacts.xml
    """
    tree = get_composite_artifacts_xml(s3_client, bucket_name, repo_prefix)
    root = tree.getroot()
    children = get_children_element(root)
    for child in children:
        location = child.get('location')
        if location is not None and location == child_location:
            children.remove(child)
    children.attrib['size'] = str(len(children))
    update_timestamp(root, new_timestamp)
    store_composite_artifacts_xml(s3_client, bucket_name, repo_prefix, tree)
    synch_compostite_artifacts_to_composite_content(s3_client, bucket_name, repo_prefix, tree)

def add_repository_to_composite(repository, bucket_name, prefix, new_child):
    """Upload a P2 repository to a prefix containing a composite repository on an S3 bucket

    :param repository: Path to the repository to upload
    :param bucket: Bucket to upload to
    :param prefix: Prefix at which the composite repository is found
    :param new_child: the extension to the prefix where the child is to be located
    :return: True if repository was uploaded, else False
    """
    try:
        session = boto3.Session()
        s3_resource = session.resource('s3')
        s3_client = session.client('s3')
        bucket = s3_resource.Bucket(bucket_name)
        S3Utils.upload_repository(repository, s3_client, bucket, urljoin(prefix, new_child))
        timestamp = int(round(time.time() * 1000.0))
        add_child_to_composite_artifacts(s3_client, bucket.name, prefix, new_child, timestamp)
    except ClientError as e:
        logger.error(e)
        return False
    return True

def remove_repository_from_composite(bucket_name, prefix, child):
    """Remove a child from a P2 composite repository

    :param bucket: Bucket to upload to
    :param prefix: Prefix at which the composite repository is found
    :param child: the extension to the prefix where the child is to be located
    :return: True if repository was uploaded, else False
    """
    try:
        session = boto3.Session()
        s3_resource = session.resource('s3')
        s3_client = session.client('s3')
        bucket = s3_resource.Bucket(bucket_name)
        timestamp = int(round(time.time() * 1000.0))
        remove_child_from_composite_artifacts(s3_client, bucket.name, prefix, child, timestamp)
        S3Utils.remove_repository(s3_client, bucket, urljoin(prefix, child))
    except ClientError as e:
        logger.error(e)
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

