
from src.data_loader.data_loader import DataLoaderError
from src.data_loader.data_loader_factory import DataLoaderFactory
from src.db.mediator_db import db
from src.query_parser.mediator_query import MediatorQuery
from src.query_parser.url_replacement_visitor import to_table_name


def rewrite_query(username, query, in_transaction):
    md_query = MediatorQuery(query)
    translated_sql = md_query.sql

    if md_query.is_md_fetch_data_statement():
        # get fetch URL
        url = md_query.get_url_from_md_fetch_data_statement()
        print ('URL: ', url)

        if not db.data_exists_for_urls([url]):
            # start a new process to load data
            data_loader = DataLoaderFactory.create_loader(url, to_table_name(url), username)
            if data_loader:
                # save 'Loading' into the md_data_status table
                db.create_new_data_status(url, username, to_table_name(url))

                # load data in *** ANOTHER PROCESS/THREAD *** for non-blocking
                data_loader.load()
            else:
                raise DataLoaderError(f"No data loader was found for {url}")

        translated_sql = f"SELECT * FROM md_v_data_status WHERE url='{url}'"

    return translated_sql
