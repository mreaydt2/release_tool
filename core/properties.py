import yaml
import logging

logger = logging.getLogger(__name__)

def get_proeprties(properties_file_path):

    logger.info(f'reading property file: {properties_file_path}')
    with open(properties_file_path) as properties:
        property_details = yaml.load(properties, loader=yaml.FullLoader)
        logger.debug(f'Properties found in file: {property_details}')
        return property_details