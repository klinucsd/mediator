from abc import ABC, abstractmethod


class MediatorQuery():
    def __init__(self, query):
        self.query = query

    @abstractmethod
    def get_ast(self):
        pass

    @abstractmethod
    def is_fetch_data_statement(self):
        pass

    @abstractmethod
    def get_fetch_url(self):
        pass