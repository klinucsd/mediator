import json

import psycopg2
from decouple import config
from faker import Faker
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool


class MediatorDatabase():
    def __init__(self):
        """
        Initializes a MediatorDB instance with a connection pool.

        The connection pool is configured with parameters from the configuration.

        Configurations:
        - minconn: Minimum number of connections in the pool.
        - maxconn: Maximum number of connections in the pool.
        - host: Database host address.
        - database: Database name.
        - user: Database user.
        - password: Database password.
        - port: Database port.

        Connection pool is created using the ThreadedConnectionPool.

        Returns:
            None
        """

        # Setup a connection pool
        self.connection_pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=config('max_connections'),
            host=config('db_host'),
            database=config('db_name'),
            user=config('db_user'),
            password=config('db_password'),
            port=config('db_port'),
        )

    def data_exists_for_urls(self, urls):
        """
        Checks if data exists in the md_data_status table for the given list of URLs.

        Args:
            urls (list): List of URLs to check for data existence.

        Returns:
            bool: True if data exists for any URL, False otherwise.
        """

        exist = False

        # Grab a connection from the pool and save data
        with self.connection_pool.getconn() as connection:
            with connection.cursor() as cursor:
                # Create a parameterized query with an IN clause
                query = sql.SQL(
                    "SELECT * FROM md_data_status WHERE url = ANY(%s) AND (status='Saved' OR status='Loading') ")

                # Execute the prepared statement with the array as a parameter
                cursor.execute(query, (urls,))

                # Fetch the results
                results = cursor.fetchall()

                # Process the results
                for row in results:
                    exist = True
                    break

                # Commit changes and close connections
                connection.commit()
                self.connection_pool.putconn(connection)

        return exist

    def create_new_data_status(self, url, username, table_name):
        """
        Creates a new entry in the md_data_status table with the provided details.

        Args:
            url (str): The URL for which data status is being created.
            username (str): The username of the user requesting data.
            table_name (str): The name of the table associated with the URL.

        Returns:
            None
        """

        # Grab a connection from the pool and save data
        with self.connection_pool.getconn() as connection:
            with connection.cursor() as cursor:
                # values for insertion
                data_to_insert = {
                    'url': url,
                    'table_name': table_name,
                    'status': 'Loading',
                    'fetch_requested_user': username
                }

                # Create an INSERT statement
                insert_query = """
                            INSERT INTO md_data_status(url, table_name, status, fetch_requested_user)
                            VALUES (%(url)s, %(table_name)s, %(status)s, %(fetch_requested_user)s)
                        """

                # Execute the INSERT statement with the provided data
                cursor.execute(insert_query, data_to_insert)

                # Commit the transaction to persist the changes
                connection.commit()
                self.connection_pool.putconn(connection)

    def update_data_status(self, url, status):
        """
        Updates the status of a data entry in the md_data_status table.

        Args:
            url (str): The URL for which data status is being updated.
            status (str): The new status to be set for the given URL.

        Returns:
            None
        """

        # Grab a connection from the pool and save data
        with self.connection_pool.getconn() as connection:
            with connection.cursor() as cursor:
                # Define UPDATE SQL statement
                update_sql = "UPDATE md_data_status SET status = %s, status_updated_time=now() WHERE url = %s;"

                # Execute the SQL statement
                cursor.execute(update_sql, (status, url))

                # Commit the transaction
                connection.commit()
                self.connection_pool.putconn(connection)

    def set_loading_error(self, url, error_message):
        """
        Set the status of a data entry in the md_data_status table to 'Error'.

        Args:
            url (str): The URL for which data status is being updated.
            error_message (str): The error message.

        Returns:
            None
        """

        # Grab a connection from the pool and save data
        with self.connection_pool.getconn() as connection:
            with connection.cursor() as cursor:
                # Define UPDATE SQL statement
                update_sql = "UPDATE md_data_status SET status = 'Error', notes=%s, status_updated_time=now() WHERE url = %s AND status='Loading';"

                # Execute the SQL statement
                cursor.execute(update_sql, (error_message, url))

                # Commit the transaction
                connection.commit()
                self.connection_pool.putconn(connection)

    def get_invalid_urls(self, urls):
        """
        Get all URLs without the status 'Saved'

        Args:
            urls (list): List of URLs.

        Returns:
            list: URLs without the status 'Saved'
        """
        if not urls:
            return []

        # Grab a connection from the pool and save data
        with self.connection_pool.getconn() as connection:
            with connection.cursor() as cursor:
                # Create a parameterized query with an IN clause
                query = """
                    SELECT checked_url
                    FROM unnest(%s) AS checked_url
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM md_data_status
                        WHERE md_data_status.url = checked_url AND md_data_status.status = 'Saved'
                    );
                """

                # Get the fully substituted and escaped query string
                # substituted_query = cursor.mogrify(query, (urls,))

                # Execute the prepared statement with the array as a parameter
                cursor.execute(query, (urls,))

                # Fetch all the URLs with status not 'Saved'
                invalid_tables = [row[0] for row in cursor.fetchall()]

                # Commit changes and close connections
                connection.commit()
                self.connection_pool.putconn(connection)

                return invalid_tables

        return url_tables

    def update_last_used_times(self, urls):
        """
        Updated the last_used_time for urls to now

        Args:
            urls (list): List of URLs.
        """

        # Grab a connection from the pool and save data
        with self.connection_pool.getconn() as connection:
            with connection.cursor() as cursor:
                # Create a parameterized statement for updating
                query = """
                    UPDATE md_data_status
                    SET last_used_time = now()
                    WHERE url = ANY(%s);
                """

                # Execute the statement
                cursor.execute(query, (urls,))

                # Commit changes and close connections
                connection.commit()
                self.connection_pool.putconn(connection)

    def notify_data_load(self, url, username, table_name):
        with self.connection_pool.getconn() as connection:
            connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            with connection.cursor() as cursor:
                message = {
                    'url': url,
                    'username': username,
                    'table_name': table_name
                }
                cursor.execute(f"NOTIFY {config('data_load_notify_channel')}, "
                               f"'{json.dumps(message)}';")

                # Commit changes and close connections
                connection.commit()
                self.connection_pool.putconn(connection)

    def save_fake_data(self, table_name):
        """
        Saves fake data into a specified table.

        Args:
            table_name (str): The name of the table where fake data will be saved.

        Returns:
            None
        """

        # Grab a connection from the pool and save data
        with self.connection_pool.getconn() as connection:
            with connection.cursor() as cursor:
                # create table statement
                create_sql = f'''
                    CREATE TABLE {table_name} (
                        id Serial primary key,
                        name VARCHAR(50),
                        email VARCHAR(64),
                        address VARCHAR(512)
                    )
                '''

                # Execute the create_sql SQL statement
                cursor.execute(create_sql)

                # Generate fake data
                fake = Faker()
                data = []
                for _ in range(100):
                    name = fake.name()
                    email = fake.email()
                    address = fake.address()
                    data.append((name, email, address))

                # Prepare an INSERT statement
                insert_query = sql.SQL(
                    f"INSERT INTO {table_name} (name, email, address) VALUES (%s, %s, %s)"
                )

                # Execute the bulk INSERT with the prepared statement
                cursor.executemany(insert_query, data)

                # Commit changes and close connections
                connection.commit()
                self.connection_pool.putconn(connection)


db = MediatorDatabase()
