import re

import pglast
from pglast.stream import IndentedStream

from src.query_parser.url_replacement_visitor import URLReplacementVisitor, is_valid_url


class MediatorQuery():
    """
     Represents a mediator query for processing Mediator Query statements.

     Attributes:
         query (str): The original SQL query.
         ast (pglast.Node): The abstract syntax tree (AST) representation of the SQL query.
         sql (str): The SQL representation of the AST.
         url_to_table_mapping (dict): A mapping of URLs to corresponding table names.
     """

    def __init__(self, query):
        """
        Initializes a MediatorQuery instance and translate a mediator query into a SQL query.

        Args:
            query (str): The original SQL query.
        """
        self.query = query

        # Translate the query into a sql (without processing md functions)
        self.ast = pglast.parse_sql(self.query)
        visitor = URLReplacementVisitor()
        visitor(self.ast)
        self.sql = str(IndentedStream(comma_at_eoln=True)(self.ast))
        self.url_to_table_mapping = visitor.url_to_table_mapping

    def is_md_fetch_data_statement(self):
        """
        Checks if the query is an md_fetch_data statement.

        Returns:
            bool: True if the query is an md_fetch_data statement, False otherwise.
        """

        pattern = r"\s*SELECT\s+md_fetch_data\s*\(\s*'([^']+)'\s*\)\s*"
        match = re.match(pattern, self.query, re.IGNORECASE)
        if match:
            url = match.group(1)
            if is_valid_url(url):
                return True
                # md_query = MediatorQuery(f"SELECT md_fetch_data('{url}')")
                # if self.ast == md_query.ast:
                #     return True
        return False

    def get_url_from_md_fetch_data_statement(self):
        """
        Extracts the URL from a md_fetch_data statement.

        Returns:
            str or None: The URL if found, None otherwise.
        """
        pattern = r"\s*SELECT\s+md_fetch_data\s*\(\s*'([^']+)'\s*\)\s*"
        match = re.match(pattern, self.query, re.IGNORECASE)
        if match:
            url = match.group(1)
            if is_valid_url(url):
                return url
        return None
