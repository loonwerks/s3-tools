'''
s3tools.P2CompositeUtils -- shortdesc

s3tools.P2CompositeUtils is a description

To be used with xml.etree.elementtree, handles reading, writing, and updating
the contents of an Eclipse P2 composite repository compositeArtifacts.xml and
compositeContent.xml specification.

@copyright:  2020 Collins Aerospace. All rights reserved.

@license:    BSD 3-Clause License
'''
import time

import xml.etree.ElementTree as ElementTree

EMPTY_COMPOSITE_ARTIFACTS_XML = '''
<?xml version='1.0' encoding='UTF-8'?>
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

EMPTY_COMPOSITE_CONTENT_XML = '''
<?xml version='1.0' encoding='UTF-8'?>
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

def get_properties(element_tree_root):
    properties = element_tree_root.findall('properties')
    if len(properties) < 1:
        return ElementTree.SubElement(element_tree_root, 'properties', {'size' : '0'})
    return properties[0]

def get_children(element_tree_root):
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
        props.attrib['value'] = timestamp

def get_child_locations(element_tree_root):
    return [child.attrib['location'] for child in get_children(element_tree_root).findall('child') if 'location' in child.attrib]

def add_child(element_tree_root, location):
    children = get_children(element_tree_root)
    children.attrib['size'] = str(len(children) + 1)
    ElementTree.SubElement(children, 'child', {'location' : location})

def write_to_string(element_tree_root):
    return ElementTree.tostring(element_tree_root, encoding='UTF-8', method='xml').decode()

def read_from_string(xml_str):
    return ElementTree.fromstring(xml_str)
