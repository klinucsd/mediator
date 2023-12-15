import importlib

from decouple import config, Csv


def __get_loader__(class_path):
    """
    Dynamically retrieves a data loader class based on the specified class path.

    Args:
        class_path (str): The full path of the data loader class (module.Class).

    Returns:
        type: The data loader class object.
    """

    # Split the class path into module and class names
    module_name, class_name = class_path.rsplit('.', 1)

    # Import the module dynamically
    module = importlib.import_module(module_name)

    # Access the class from the module
    data_loader_class = getattr(module, class_name)

    # data_loader_class is the actual class object
    return data_loader_class


class DataLoaderFactory():

    @staticmethod
    def create_loader(url, table_name, username):
        """
        Creates a data loader instance based on the specified URL, table name, and username.

        Iterates through all the data loaders specified in the .env file and returns the first
        data loader that can validate the given URL.

        Args:
            url (str): The URL for data loading.
            table_name (str): The name of the table to store the loaded data.
            username (str): The username associated with the data loader.

        Returns:
            DataLoader or None: An instance of a data loader class that can handle the specified URL,
                                or None if no suitable data loader is found.
        """

        # Iterate through all the data loaders specified in .env
        for class_path in config('data_loaders', default='', cast=Csv()):
            # Convert string to a real class
            data_loader_class = __get_loader__(class_path)

            # If the data loader can process the URL, use it
            if data_loader_class.validate(url):
                # Use the data loader
                return data_loader_class(url, table_name, username)
        return None
