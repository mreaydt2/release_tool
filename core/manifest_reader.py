import xml.etree.ElementTree as ET
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def _parse_xml(xmlfile):
    # create element tree object
    logger.debug(str(xmlfile))
    tree = ET.parse(Path(xmlfile))

    root = tree.getroot()
    change_items = []

    for item in root.findall('./include'):
        change_items.append(item.attrib['file'])
    return change_items


def read_xml_files(xml_file, source_file_directory):
    """
    Takes an xml file and returns a dictionary of the content
    :param xml_file:
    :param source_file_directory:
    :return:
    """
    files = _parse_xml(Path(xml_file))
    files_for_release = {}
    for i, file_loc in enumerate(files):
        change_file = Path(source_file_directory, file_loc)
        sql = open(change_file, 'r')
        files_for_release[file_loc] = sql.read()
    return files_for_release
