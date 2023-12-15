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
                query = sql.SQL("SELECT * FROM md_data_status WHERE url = ANY(%s)")

                # Execute the prepared statement with the array as a parameter
                cursor.execute(query, (urls,))

                # Fetch the results
                results = cursor.fetchall()

                # Process the results
                for row in results:
                    exist = True
                    break

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


db = MediatorDatabase()
