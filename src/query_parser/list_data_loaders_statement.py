import re

from decouple import config, Csv

from src.data_loader.data_loader_factory import __get_loader__
from src.query_parser.mediator_query import MediatorQuery


class ListDataLoadersStatement():
    def __init__(self, md_query: MediatorQuery):
        if self.validate(md_query.query):
            self.query = md_query.query
        else:
            raise ListDataLoadersStatementError('Not a mediator list data loader statement.')

    @staticmethod
    def validate(query):
        """
        Checks if the query is a md_list_data_loaders statement.

        Returns:
            bool: True if the query is a md_list_data_loaders statement, False otherwise.
        """
        pattern = r"\s*SELECT\s+md_list_data_loaders\s*\(\s*\)\s*"
        match = re.match(pattern, query, re.IGNORECASE)
        if match:
            return True
        else:
            return False

    @staticmethod
    def to_sql():
        # Iterate through all the data loaders specified in .env
        data_loaders = []
        for class_path in config('data_loaders', default='', cast=Csv()):
            # Convert string to a real class
            data_loaders.append(__get_loader__(class_path))

        # Check if the list is not empty
        if data_loaders:
            # Construct the VALUES clause string
            values_clause = ",\n".join(
                f"('{loader.get_name()}', '{loader.get_description()}')"
                for loader in data_loaders
            )

            # Return the final SQL string
            return f"""
                SELECT * 
                FROM (VALUES {values_clause}) AS md_data_loaders(name, description)
            """
        else:
            return """
                SELECT * 
                FROM (VALUES (NULL::text, NULL::text)) AS md_data_loaders(name, description) 
                WHERE FALSE 
            """


class ListDataLoadersStatementError(Exception):
    """
        Custom exception class for ListDataLoadersStatement-related errors.
    """
    pass
