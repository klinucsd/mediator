import re

from src.data_loader.data_loader import DataLoaderError
from src.data_loader.data_loader_factory import DataLoaderFactory
from src.db.mediator_db import db
from src.query_parser.mediator_query import MediatorQuery
from src.query_parser.url_replacement_visitor import is_valid_url, to_table_name


class FetchDataStatement():
    def __init__(self, md_query: MediatorQuery):
        if self.validate(md_query.query):
            self.query = md_query.query
            self.url = self.__get_url(md_query.query)
        else:
            raise FetchDataStatementError('Not a mediator fetch data statement.')

    @staticmethod
    def validate(query):
        """
        Checks if the query is a md_fetch_data statement.

        Returns:
            bool: True if the query is a md_fetch_data statement, False otherwise.
        """
        pattern = r"\s*SELECT\s+md_fetch_data\s*\(\s*'([^']+)'\s*\)\s*"
        match = re.match(pattern, query, re.IGNORECASE)
        if match:
            url = match.group(1)
            if is_valid_url(url):
                return True
        return False

    @staticmethod
    def __get_url(query):
        """
        Extracts the URL from a md_fetch_data statement.

        Returns:
            str or None: The URL if found, None otherwise.
        """
        pattern = r"\s*SELECT\s+md_fetch_data\s*\(\s*'([^']+)'\s*\)\s*"
        match = re.match(pattern, query, re.IGNORECASE)
        if match:
            url = match.group(1)
            if is_valid_url(url):
                return url
        return None

    def fetch_data(self, username):
        # Check if data for the URL already exists in the database
        if not db.data_exists_for_urls([self.url]):
            # Start a new process to load data
            data_loader = DataLoaderFactory.create_loader(self.url, to_table_name(self.url), username)

            # If a data loader is found, proceed with loading data
            if data_loader:
                # Save 'Loading' status into the md_data_status table
                db.create_new_data_status(self.url, username, to_table_name(self.url))

                # Load data in *** ANOTHER PROCESS/THREAD *** for non-blocking
                data_loader.load()
            else:
                # Raise an error if no data loader is found for the URL
                raise DataLoaderError(f"No data loader was found for {url}")


class FetchDataStatementError(Exception):
    """
        Custom exception class for FetchDataStatement-related errors.
    """
    pass
