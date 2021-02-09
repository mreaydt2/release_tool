from core import manifest_reader as filereader, snowflake_manager as sfm
from pathlib import Path
import datetime
import logging.config
import yaml
import sys


def setup_logging(default_path='config/logging_config.yaml'):
    """
    Sets the logging for the application
    :param default_path: Logging config location
    """
    path = default_path
    with open(path, 'rt') as f:
        log_cfg = yaml.safe_load(f.read())
    logging.config.dictConfig(log_cfg)


logger = logging.getLogger(__name__)

change_log_status = []

halt_release_on_fail = True


class DeployChanges(object):
    """
    Deploys changes found in the manifest files, according to the variables in the properties files and run time
    parameters
    """
    def __init__(self, target_database, cloning, properties):
        self.changes_deployed = []
        self.change_log_directory = properties.get('change_log_directory')
        self.master_change_log_name = properties('master_change_log_name')
        self.master_change_log_file = Path(self.change_log_directory, self.master_change_log_name)
        self.root_sql_directory = properties.get('root_sql_directory')
        self.properties = properties
        self.target_database = target_database
        self.cloning = cloning
        self.snowflake_manager = DeployChanges.get_snowflake_manager(target_database=target_database,
                                                                     properties=properties)

    @staticmethod
    def get_snowflake_manager(target_database, **properties):
        """
        Creates a Snowflake Manager instance based on the properties connection details
        :param target_database: Target database for release
        :param properties: Dictionary of teh properties yaml file
        """
        sf = sfm.SnowflakeManager(conn=sfm.get_conn(properties),
                                  target_database=target_database,
                                  history_schema=properties.get('history_schema'),
                                  history_table=properties.get('history_table'))
        return sf

    def _deployable_changes(self):
        """
        Checks the changes in the change log file against changes in the database history tables
        changes that are not successfully released to teh history table are deployable changes
        """
        for change_history in self.snowflake_manager.get_database_change_history():
            self.changes_deployed.append(change_history[4]) \
                if change_history[4] not in self.changes_deployed else self.changes_deployed

        change_log_files = filereader.read_xml_files(xml_file=self.master_change_log_file,
                                                     source_file_directory=self.change_log_directory)
        return change_log_files

    def _clone_target(self):
        """
        If the cloning variable is true, will clone the target database and release to the clone
        """
        if self.cloning:
            logger.info(f'Cloning {self.target_database}')
            self.snowflake_manager.clone_database(target_database=self.target_database)
        else:
            logger.info((f'Deploying directly to {self.target_database}, clongin parameter set to {str(self.cloning)}'))
            self.snowflake_manager.deploy_database_name = self.target_database

    def deploy_sql_change_set(self, change_log_file):
        """
        Releases the SQL files in the change manifest file in the order listed within the file
        :param change_log_file: File name containing a list of the sql files for release
        """
        sql_files = filereader.read_xml_files(xml_file=Path(self.change_log_directory, change_log_file),
                                              source_file_directory=self.root_sql_directory)
        logger.debug(f'Starting to extract sql files from {change_log_file}')

        for key in sql_files:
            # database error 0 for no error, 1 is an error
            if self.snowflake_manager.database_error == 0:
                change_metadata = self.snowflake_manager.get_change_details(sqlfile=f'{self.root_sql_directory}/{key}',
                                                                            date_released=datetime.datetime.now(),
                                                                            change_log=change_log_file)

                # track_change_in_history_table returns true if successful, false if error
                if self.snowflake_manager.track_change_in_history_table(**change_metadata):

                    # deploy_change_to_target sets snowflake_manager.database_error
                    self.snowflake_manager.deploy_change_to_target(sqlfile=f'{sql_files[key]}',
                                                                   database=self.target_database,
                                                                   author=change_metadata['author'],
                                                                   id=change_metadata['id'])
                    if self.snowflake_manager.database_error == 0:
                        self.snowflake_manager.set_change_status(status='success',
                                                                 id=f"{change_metadata['id']}")
                    elif self.snowflake_manager.database_error == 1:
                        self.snowflake_manager.set_change_status(status='failed',
                                                                 id=f"{change_metadata['id']}")
        # record the status of database_error for the sql file. this allows releases to stop or continue if a
        # sql file fails.
        change_log_status.append((int(self.snowflake_manager.database_error)))

    def deploy_release(self):
        """
        Reads the feature manifest file and releases the changes in the order listed.
        """
        setup_logging()

        logger.info(f'Starting to deploy changes for database {self.target_database}')

        # Will clone the target database if the clone parameter is True
        self._clone_target()

        changes_deployed = []

        # Get changes from file not already released to the database
        deployable_change_files = self._deployable_changes()

        for log_file in deployable_change_files:
            if log_file not in changes_deployed and self.snowflake_manager.database_error == 0:
                logger.info(f'Changelog {log_file} found..')

                self.deploy_sql_change_set(change_log_file=log_file)

                if self.snowflake_manager.database_error == 1:
                    logger.info(f'Failed to release: {log_file}')
                else:
                    logger.info(f'Finished: {log_file}')

            if halt_release_on_fail and 1 in change_log_status:
                logger.error('Stopping release: halt_release_on_fail is True')
                sys.exit(1)

        if 1 in change_log_status:
            logger.info('ATTENTION: Databases not swapped because of errors')
