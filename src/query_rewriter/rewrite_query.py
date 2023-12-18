# ------------------------------------------------------------------------------
# The following code sets the PYTHONPATH of the code in the pgBouncer environment.
# pgBouncer uses the Cython interpreter to run the mediator's Python code,
# but pgBouncer does not pass the value of the environment variable PYTHONPATH to Cython.
# So this code is needed to set the location of the Python code.
import sys

from decouple import config, UndefinedValueError

try:
    sys.path.append(config('python_code_home'))
except UndefinedValueError:
    pass
# End of Setting
# ------------------------------------------------------------------------------

from threading import Thread

from src.db.mediator_db import db
from src.query_parser.fetch_data_statement import FetchDataStatement
from src.query_parser.mediator_query import MediatorQuery
from src.query_parser.list_data_loaders_statement import ListDataLoadersStatement


def rewrite_query(username, query, in_transaction):
    """
    Rewrite the given query to a translated SQL and implement the functions in the query.
    This function is required by pgBouncer-rr and will be called whenever a query is sent
    to the mediator.

    Args:
        username (str): The username who sent the query.
        query (str): The original mediator query.
        in_transaction (bool): Flag indicating whether the query is within a transaction.

    Returns:
        str: The translated SQL query.
    """

    # Parse the original query and translate it
    md_query = MediatorQuery(query)
    translated_sql = md_query.sql

    # Check if the query is "SELECT md_fetch_url(URL)" statement
    if FetchDataStatement.validate(query):
        # Construct a FetchDataStatement
        fetch_data_statement = FetchDataStatement(md_query)

        # Try to fetch data in a separate thread, but got errors when start a new thread. 
        # Thread(target=fetch_data_statement.fetch_data, args=[username]).start()
        fetch_data_statement.fetch_data(username)

        # Modify the translated SQL to query the md_v_data_status table for the specific URL
        translated_sql = f"SELECT * FROM md_v_data_status WHERE url='{fetch_data_statement.url}'"

    # Check if the query is "SELECT md_list_data_loaders()" statement
    elif ListDataLoadersStatement.validate(query):
        # Construct a ListDataLoadersStatement
        list_data_loaders_statement = ListDataLoadersStatement(md_query)

        # Modify the translated SQL
        translated_sql = list_data_loaders_statement.to_sql()

    # Do something similar for other statements

    # Validate that all the URLs used in the query are ready to query
    else:
        # Get all the URLs used in the query
        urls = list(md_query.url_to_table_mapping.keys())
        if urls:
            # Get all invalid URLs
            invalid_urls = db.get_invalid_urls(urls)
            if not invalid_urls:
                # All the URLs are valid. Update the last used times for URLs
                db.update_last_used_times(urls)
            else:
                # Some invalid URLs exist
                error_message = f'The following URLs are not ready to query: {", ".join(invalid_urls)}'
                return f"SELECT md_mediator_error('{error_message}');"

    return translated_sql
