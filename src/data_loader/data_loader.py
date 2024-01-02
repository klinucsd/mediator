from abc import ABC, abstractmethod

import psycopg2
from decouple import config


class DataLoader(ABC):
    def __init__(self, url, table_name, username):
        """
        Initializes the DataLoader instance with essential attributes.

        Args:
            url (str): The URL for data loading.
            table_name (str): The name of the table to store the loaded data.
            username (str): The username associated with the data loader.
        """
        self.url = url
        self.table_name = table_name
        self.username = username

    @staticmethod
    @abstractmethod
    def get_name() -> str:
        """
        Get the name of this data loader.

        Return:
            str: the name of this data loader
        """
        pass

    @staticmethod
    @abstractmethod
    def get_description() -> str:
        """
        Get the description of this data loader.

        Return:
            str: the description of this data loader
        """
        pass

    @staticmethod
    @abstractmethod
    def validate(url) -> bool:
        """
        Validates whether this data loader can process the data at the given URL.

        Args:
            url (str): The URL to validate.

        Returns:
            bool: True if the loader can process the data, False otherwise.
        """
        pass

    @abstractmethod
    def load(self):
        """
         Loads data from the specified URL into the associated table and updates the status.

         Steps:
         1. Load data from self.url and save it to the table self.table_name.
         2. Update the status column of the table md_data_status for the URL to 'Saved'.
         3. Delete the record for this URL if any exception occurs during the process.
         """
        pass

    @staticmethod
    def set_loading_error(url, error_message):
        """
            Set the status of a data entry in the md_data_status table to 'Error'.
            It is not safe to share a connection pool with multiple processes.
            This method opens and uses a new connection in the process

            Args:
                error_message (str): The error message.

            Returns:
                None
        """
        with psycopg2.connect(host=f"{config('db_host')}", dbname=f"{config('db_name')}",
                              user=f"{config('db_user')}", password=f"{config('db_password')}") as conn:
            with conn.cursor() as cursor:
                # Define UPDATE SQL statement
                update_sql = """
                    UPDATE md_data_status 
                       SET status = 'Error', notes=%s, status_updated_time=now() 
                     WHERE url = %s AND status='Loading';
                """

                # Execute the SQL statement
                cursor.execute(update_sql, (error_message, url))

                # Commit the transaction
                conn.commit()

    @staticmethod
    def update_data_status(url, status):
        """
            Updates the status of self.url in the md_data_status table.
            It is not safe to share a connection pool with multiple processes.
            This method opens and uses a new connection in the process

            Args:
                status (str): The new status to be set for the given URL.

            Returns:
                None
        """
        with psycopg2.connect(host=f"{config('db_host')}", dbname=f"{config('db_name')}",
                              user=f"{config('db_user')}", password=f"{config('db_password')}") as conn:
            with conn.cursor() as cursor:
                # Define UPDATE SQL statement
                update_sql = "UPDATE md_data_status SET status = %s, status_updated_time=now() WHERE url = %s;"

                # Execute the SQL statement
                cursor.execute(update_sql, (status, url))

                # Commit the transaction
                conn.commit()


class DataLoaderError(Exception):
    """
        Custom exception class for DataLoader-related errors.
    """
    pass
