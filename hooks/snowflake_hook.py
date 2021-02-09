import snowflake.connector
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


class SnowflakeConnection:
    """
    Interact with Snowflake.
    """
    def __init__(self, **kwargs):
        self.user = kwargs.get("user")
        self.password = kwargs.get("password")
        self.warehouse = kwargs.get("warehouse")
        self.database = kwargs.get("database")
        self.account = kwargs.get("account")
        self.role = kwargs.get("role")
        self.authenticator = kwargs.get("authenticator")
        self.private_key_file = kwargs.get("private_key")
        self.private_key = ''

    def _get_conn_params(self):
        """
        one method to fetch connection params as a dict
        """
        conn_config = {
            "user": self.user,
            "password": self.password or '',
            "database": self.database or '',
            "account": self.account or '',
            "warehouse": self.warehouse or '',
            "role": self.role or '',
            "authenticator": self.authenticator or ''
        }

        if self.private_key_file:
            conn_config['private_key'] = self._get_private_key()

        return conn_config

    def _get_private_key(self):
        """
        Uses the environment variable SNOWFLAKE_PRIVATE_KEY to get the passphrase
        THis can be customised or overloaded to pick up the passphase from somewhere different
        :return: private key
        """
        with open(self.private_key_file, "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=os.environment['SNOWFLAKE_PRIVATE_KEY'].encode(),
                backend=default_backend()
            )

        private_key = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encrytion_algorithm=serialization.NoEncryption())

        return private_key

    def get_conn(self):
        """
        Returns a snowflake.connection object
        """
        conn_config = self._get_conn_params()
        conn = snowflake.connector.connect(**conn_config)
        return conn

    def set_autocommit(self, conn, autocommit):
        conn.autocommit(autocommit)
