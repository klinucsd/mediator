from abc import ABC, abstractmethod


class MediatorDatabase(ABC):
    def __init__(self):
        # setup a connection pool
        self.connection_pool = None

    @abstractmethod
    def save_data_status(self, url, username, status):
        # grab a connection from the pool and save data
        pass

    @abstractmethod
    def data_exists(self, url):
        # grab a connection from the pool and save data
        pass


db = MediatorDatabase()