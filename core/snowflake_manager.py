from connectors import snowflake_connector as sfc
import datetime
import logging
import random
import string
from snowflake.connector.errors import DatabaseError, ProgrammingError

logger = logging.getLogger(__name__)

class SnowflakeManager:

    def __init__(self, conn, target_database):
        self.history_schema = 'HISTORY_SCHEMA'
        self.history_table = 'HISTORY_TABLE'
        self.change_history = []
        self.deploy_database_name = ''
        self.database_error = 0
        self.conn = conn
        self.target_database = target_database

    @staticmethod
    def get_conn(properties):
        conn = sfc.SnowflakeConnection(properties)
        return conn

    def _execute_sql(self, sql):
        try:
            return self.conn.cursor().execute(sql)
        except ProgrammingError as e:
            logger.error(e)
            self.database_error = 1
        except DatabaseError as e:
            logger.error(e)
            self.database_error = 1
        except Exception as e:
            raise e

    def get_database_chnage_history(self):
        if self.deploy_database_name is None:
            database = self.target_database
        else:
            database = self.deploy_database_name

        self._validate_chnage_history_table(database=database)

        sql = f"SELECT * FROM {database}.{self.history_schema}.{self.history_table} " \
              f"WHERE status = 'success'"
        logger.debug(sql)

        self.change_history = self._execute_sql(sql).fetchall()

        return self.change_history

    def track_change_in_history_table(self, **kwargs):

        sql = f"INSERT INTO {self.deploy_database_name}.{self.history_schema}.{self.history_table} " \
              f"(id, author, filename, date_released, change_log, jira_number, release_number, comments, " \
              f"deployment_id, status) " \
              f"VALUES(\'{kwargs.get('id')}\'," \
              f"\'{kwargs.get('author')}\'," \
              f"\'{kwargs.get('filename')}\'," \
              f"\'{kwargs.get('date_released')}\'," \
              f"\'{kwargs.get('change_log')}\'," \
              f"\'{kwargs.get('jira_number')}\'," \
              f"\'{kwargs.get('release_number')}\','" \
              f"\'{kwargs.get('comments')}\'," \
              f"\'{kwargs.get('deployment_id')}\'," \
              f"\'{kwargs.get('status')}\'" \
              f")"
        try:
            if self._check_id_is_valid(kwargs.get('id')):
                self._execute_sql(sql)
                return True
            else:
                return False
        except Exception as e:
            raise e

    def _check_id_id_valid(self, id: str):
        is_valid = True
        for x in self.change_history:
            if x[0] == id and x[9] == 'success':
                logger.info(f"Chnage ID {id} is already released to this database")
                is_valid = False
            elif x[0] == id and x[9] == 'failed':
                logger.info(f"Change ID {id} was a failed release to this database, re-trying release")
                is_valid = True

        return is_valid

    def deploy_change_to_target(self, sqlfile: str, author: str, id: str, database: str = None):
        if database is None:
            database = self.deploy_database_name

        use_db = f'USE DATABASE {database};\n'
        sql = use_db + sqlfile

        try:
            logger.debug(f'SQL to release: \n {sqlfile}')
            self.conn.execute_string(sql, remove_comments=True, return_cursors=True)
            logger.info(f'Released change {author}:{id}')

        except ProgrammingError as e:
            logger.error(e)
            logger.info(f'Failed to release change {author}:{id}, check errors')
            logger.error(f'SQL to release: \n {sqlfile}')
            self.database_error = 1
        except DatabaseError as e:
            logger.error(e)
            logger.info(f'Failed to release change {author}:{id}, check errors')
            logger.error(f'SQL to release: \n {sqlfile}')
            self.database_error = 1
        except Exception as e:
            raise e

    @staticmethod
    def get_change_details(sqlfile: str, date_released: datetime, change_log: str):

        with open(sqlfile) as f:
            content = f.readlines()
            logger.debug(f'Found SQL file {sqlfile}')
        content = [x.strip() for x in content]

        change_details = {'filename': sqlfile,
                          'date_released': date_released,
                          'deployment_id': 1,
                          'change_log': change_log,
                          'status': 'in progress'
        }

        for i, line in enumerate(content):
            if '--chnageset' in line:

                author = line.split(" ")[1][:line.split(" ")[1].find(':')]
                id = line.split(" ")[1][line.split(" ")[1].find(':') + 1:]
                context = line.split(" ")[2][line.split(" ")[2].find(':') + 1:]

                change_details['author'] = author
                change_details['id'] = id
                change_details['release_number'] = context

            elif 'comment:' in line and len(line) > len('--comment:'):
                comments = line.split(" ")[1]
                change_details['comments'] = comments

            elif 'labels:' in line:
                jira = line.split(" ")[1]
                change_details['jira_number'] = jira

        return change_details

    def clone_database(self, target_database):
        pass

    def swap_database(self, cloned_db_name: str, target_db_name: str):

        sql = f"ALTER DATABASE {cloned_db_name} SWAP WITH {target_db_name}"

        try:
            logger.info(f'Swapping clone {cloned_db_name} from {target_db_name}')
            self._execute_sql(sql)
            logger.info('Databases swapped')
        except Exception as e:
            raise e

    def mark_database_release(self, status: str):
        new_name = f'{self.deploy_database_name}_{status.upper()}'
        sql = f'ALTER DATABASE {self.deploy_database_name} RENAME TO {new_name}'
        try:
            logger.info(f'Marking clone {self.deploy_database_name} as {status.upper()}')
            self._execute_sql(sql)
            self.deploy_database_name = new_name
        except Exception as e:
            raise e

    def set_change_status(self, status: str, id: str):
        sql = f"UPDATE {self.deploy_database_name}.{self.history_schema}.{self.history_table} " \
              f"SET STATUS = '{status}' " \
              f"WHERE id = '{id}'"
        self._execute_sql(sql)

    def _validate_chnage_history_table(self, database=None):
        if database is None:
            database = self.deploy_database_name

        sql = f"select count(1) from {database}.INFORMATION_SCHEMA.TABLES " \
              f"WHERE table_catalog = '{database}' " \
              f"AND table_schema = '{self.history_schema}' " \
              f"AND table_schema = '{self.history_table}'"

        if self._execute_sql(sql).fetchone()[0] == 0:
            logger.info(f'Tracking table not found.... Creating {database}.{self.history_schema}.{self.history_table}')
            self._create_tracking_table(database=database)



