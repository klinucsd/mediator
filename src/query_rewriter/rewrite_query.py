from src.data_loader.abstract_data_loader import DataLoader
from src.db.mediator_db import db
from src.query_parser.mediator_query import MediatorQuery


def rewrite_query(username, query, in_transaction):
    md_query = MediatorQuery(query)

    if md_query.contains_mediator_functions():
        # process these md functions
        if md_query.is_fetch_data_statement():
            # get fetch URL
            url = md_query.get_fetch_url()

            if not db.data_exists(url):
                # save 'Loading' into the md_data_status table
                db.save_data_status(url, username, 'Loading')

                # start a new process to fetch data
                # in the end of the new process, update the status to 'Saved'
                data_loader = DataLoader(url, username)
                data_loader.load()

            translated_query = f"SELECT url, status FROM md_data_status WHERE url={url}";
        else:
            # process other functions
            translated_query = None
    else:
        # rewrite the query by translating URLs to hex hashcodes
        translated_query = None

    return translated_query



