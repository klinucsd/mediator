# ------------------------------------------------------------------------------
# The following code sets the PYTHONPATH of the code in the pgBouncer environment.
# pgBouncer uses the Cython interpreter to run the mediator's Python code,
# but pgBouncer does not pass the value of the environment variable PYTHONPATH to Cython.
# So this code is needed to set the location of the Python code.
import sys
from threading import Thread

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
from threading import Thread

import psycopg2

# Configure logging
from decouple import config

from src.data_loader.data_loader import DataLoaderError
from src.data_loader.data_loader_factory import DataLoaderFactory

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s UTC [%(process)d] %(levelname)s Data Loader %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def load_data(url, username, table_name):
    # Start a new process to load data
    print('does it get to the data loader?')
    data_loader = DataLoaderFactory.create_loader(url, table_name, username)

    # If a data loader is found, proceed with loading data
    if data_loader:
        data_loader.load()
    else:
        # Raise an error if no data loader is found for the URL
        raise DataLoaderError(f"No data loader was found for {url}")


async def handle_notifications():
    conn = None
    try:
        await asyncio.sleep(10)
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
                    thread = Thread(target=load_data, args=[payload['url'], payload['username'], payload['table_name']])
                    thread.start()
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
