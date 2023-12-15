from src.query_parser.fetch_data_statement import FetchDataStatement
from src.query_parser.mediator_query import MediatorQuery


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

    # Check if the query is md_fetch_data statement
    if FetchDataStatement.validate(query):
        # Construct a FetchDataStatement
        fetch_data_statement = FetchDataStatement(md_query)

        # Fetch data in a *** SEPARATE PROCESS *** (todo)
        fetch_data_statement.fetch_data(username)

        # Modify the translated SQL to query the md_v_data_status table for the specific URL
        translated_sql = f"SELECT * FROM md_v_data_status WHERE url='{fetch_data_statement.url}'"

    # Do something similar for other statements
    # Don't write the details of processing other statements here to keep this method clearn

    return translated_sql
