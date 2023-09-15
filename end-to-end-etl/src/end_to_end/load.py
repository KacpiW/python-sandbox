from typing import Tuple

from mysql.connector import Error
import mysql.connector
import boto3

from end_to_end.config import setup_logger

logger = setup_logger(__name__, 'ete.log')


class Loader:
    def __init__(self, host: str, user: str, passwd: str, db: str) -> None:
        """
        Initialize the Loader object and establish the connection to the database.
        """
        self.connection = self._create_connection(host, user, passwd, db)

    def _create_connection(self, host: str, user: str, passwd: str, db: str, port: int = 3306) -> mysql.connector.connection:
        """
        Create and return a connection to the database.
        """
        try:
            connection = mysql.connector.connect(
                host=host,
                user=user,
                port=port,
                passwd=passwd,
                database=db
            )
            logger.info("Connected to MySQL successfully")
            return connection
        except Error as e:
            logger.error(f"Error while connecting to MySQL: '{e}'")
            raise

    def insert_data(self, table_name: str, columns: str, data: Tuple) -> None:
        """
        Insert data into a given table.

        Args:
            table_name: The name of the table to insert data into.
            columns: Comma-separated column names where data should be inserted.
            data: A tuple of data to insert into the table.
        """
        sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s']*len(data))})"
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql_query, data)
            self.connection.commit()
            logger.debug(f"Data inserted into {table_name} successfully")
        except Error as e:
            logger.error(f"Failed to insert data into {table_name}: {e}")
            raise

    def __del__(self):
        """
        Close the connection when the Loader object is deleted.
        """
        if self.connection.is_connected():
            self.connection.close()
            logger.info("MySQL connection closed")


def copy_object_between_buckets(extract: str, load: str, file_name: str) -> None:
    """
    Copy an object between two buckets.
    """
    # Create a session using your AWS credentials
    s3 = boto3.resource('s3')

    # Specify the source bucket and the key of the object to be copied
    old_source = {'Bucket': extract, 'Key': file_name}
    new_source = {'Bucket': load, 'Key': file_name}

    # Use the copy function to copy the object to the target bucket
    try:
        logger.info(f"Copying {file_name} from {old_source} to {new_source}")
        s3.meta.client.copy(old_source, load, file_name)
        logger.info(f"Copied {file_name} successfully")
    except Exception as e:
        logger.error(f"Failed to copy {file_name}. Reason: {e}")
        raise
