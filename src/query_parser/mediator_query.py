import re

import pglast
from pglast.stream import IndentedStream

from src.query_parser.url_replacement_visitor import URLReplacementVisitor, is_valid_url


class MediatorQuery():
    def __init__(self, query):
        self.query = query

        # translate the query into a sql (without processing md functions)
        self.ast = pglast.parse_sql(self.query)
        visitor = URLReplacementVisitor()
        visitor(self.ast)
        self.sql = str(IndentedStream(comma_at_eoln=True)(self.ast))
        self.url_to_table_mapping = visitor.url_to_table_mapping

    def is_md_fetch_data_statement(self):
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
        pattern = r"\s*SELECT\s+md_fetch_data\s*\(\s*'([^']+)'\s*\)\s*"
        match = re.match(pattern, self.query, re.IGNORECASE)
        if match:
            url = match.group(1)
            if is_valid_url(url):
                return url
        return None
