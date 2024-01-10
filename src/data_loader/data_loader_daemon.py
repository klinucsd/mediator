# ------------------------------------------------------------------------------
# The following code sets the PYTHONPATH of the code in the pgBouncer environment.
# pgBouncer uses the Cython interpreter to run the mediator's Python code,
# but pgBouncer does not pass the value of the environment variable PYTHONPATH to Cython.
# So this code is needed to set the location of the Python code.
import sys
from multiprocessing import Process

from decouple import config, UndefinedValueError

try:
    sys.path.append(config('python_code_home'))
except UndefinedValueError:
    pass
# End of Setting
# ------------------------------------------------------------------------------

import asyncio
import json
import logging

import psycopg2

# Configure logging
from decouple import config

from src.data_loader.data_loader import DataLoader
from src.data_loader.data_loader_factory import DataLoaderFactory

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d UTC [%(process)d] %(levelname)s: Data Loader: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def load_data(url, username, table_name):
    try:
        # Start a new process to load data
        data_loader = DataLoaderFactory.create_loader(url, table_name, username)

        # If a data loader is found, proceed with loading data
        if data_loader:
            data_loader.load()
        else:
            logging.error(f"No data loader was found.: {url}")
            DataLoader.set_loading_error(url, f"No data loader was found.")
    except Exception as e:
        # traceback.print_exc()
        logging.error(f"Encountered an error when loading {url}: {str(e)}.")
        DataLoader.set_loading_error(url, f'Encountered an error: {str(e)}.')
        DataLoader.drop_table(url)


async def handle_notifications():
    conn = None
    try:
        conn = psycopg2.connect(
            host=f"{config('db_host')}",
            dbname=f"{config('db_name')}",
            user=f"{config('db_user')}",
            password=f"{config('db_password')}",
        )
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute(f"LISTEN {config('data_load_notify_channel')};")

        while True:
            await asyncio.sleep(0)  # Yield control to the event loop
            conn.poll()
            for notify in conn.notifies:
                try:
                    # Process notification payload
                    logging.info(f"Received notification: {notify.payload}")
                    payload = json.loads(notify.payload)
                    # Important Note: Don't use any shared connection pool inside the process
                    # which may not be safe within multiple processes
                    process = Process(target=load_data,
                                      args=(payload['url'], payload['username'], payload['table_name']))
                    process.start()
                except Exception as e:
                    logging.error(f"Error processing notification: {e}")
            conn.notifies.clear()

    except (psycopg2.OperationalError, psycopg2.errors.UniqueViolation) as e:
        logging.error(f"Database connection error: {e}")
        # Handle connection errors, retry logic, etc.

    finally:
        if conn:
            cursor.close()
            conn.close()
            logging.info("Database connection closed")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(handle_notifications())
