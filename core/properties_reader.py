import yaml
import logging

logger = logging.getLogger(__name__)


def get_properties(properties_file_path):
    """
    Returns the properties yaml file as a dictionary
    :param properties_file_path: location of properties.yaml
    :return: Dictionary of properties
    """
    logger.info(f'reading property file: {properties_file_path}')
    with open(properties_file_path) as properties:
        property_details = yaml.load(properties, loader=yaml.FullLoader)
        logger.debug(f'Properties found in file: {property_details}')
        return property_details
