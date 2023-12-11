from abc import ABC, abstractmethod


class DataLoader(ABC):
    def __init__(self, url, table_name, username):
        self.url = url
        self.table_name = table_name
        self.username = username

    @staticmethod
    @abstractmethod
    def validate(url) -> bool:
        pass

    @abstractmethod
    def load(self):
        # step1: load data from self.url and save to the table self.table
        # step2: update the status column of the table md_data_status for the url to 'Saved'
        # delete the record for this url if any exception

        pass


class DataLoaderError(Exception):
    pass
