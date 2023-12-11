import importlib

from decouple import config, Csv


def __get_loader__(class_path):
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
        # Iterate through all the data loaders specified in .env
        for class_path in config('data_loaders', default='', cast=Csv()):
            data_loader_class = __get_loader__(class_path)
            if data_loader_class.validate(url):
                return data_loader_class(url, table_name, username)
        return None
