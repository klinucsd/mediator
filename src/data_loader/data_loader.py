from abc import ABC, abstractmethod


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


class DataLoaderError(Exception):
    """
        Custom exception class for DataLoader-related errors.
    """
    pass
