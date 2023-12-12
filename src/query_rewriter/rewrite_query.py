
from src.data_loader.data_loader import DataLoaderError
from src.data_loader.data_loader_factory import DataLoaderFactory
from src.db.mediator_db import db
from src.query_parser.mediator_query import MediatorQuery
from src.query_parser.url_replacement_visitor import to_table_name


def rewrite_query(username, query, in_transaction):
    """
    Rewrite the given query to a translated SQL and implement the functions in the query.

    Args:
        username (str): The username associated with the query.
        query (str): The original mediator query.
        in_transaction (bool): Flag indicating whether the query is within a transaction.

    Returns:
        str: The translated SQL query.
    """

    # Parse the original query and translate it
    md_query = MediatorQuery(query)
    translated_sql = md_query.sql

    # Check if the query contains an md_fetch_data statement
    if md_query.is_md_fetch_data_statement():
        # Extract the fetch URL from the md_fetch_data statement
        url = md_query.get_url_from_md_fetch_data_statement()

        # Check if data for the URL already exists in the database
        if not db.data_exists_for_urls([url]):
            # Start a new process to load data
            data_loader = DataLoaderFactory.create_loader(url, to_table_name(url), username)

            # If a data loader is found, proceed with loading data
            if data_loader:
                # Save 'Loading' status into the md_data_status table
                db.create_new_data_status(url, username, to_table_name(url))

                # Load data in *** ANOTHER PROCESS/THREAD *** for non-blocking
                data_loader.load()
            else:
                # Raise an error if no data loader is found for the URL
                raise DataLoaderError(f"No data loader was found for {url}")

        # Modify the translated SQL to query the md_v_data_status table for the specific URL
        translated_sql = f"SELECT * FROM md_v_data_status WHERE url='{url}'"

    return translated_sql
