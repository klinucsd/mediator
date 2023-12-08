
from abc import ABC, abstractmethod


class DataLoader(ABC):
    def __init__(self, url, username):
        self.url = url
        self.username = username

    @abstractmethod
    def load(self):
        # if the url is from ArcGIS Feature, then use ArcGISFeatureDataLoader
        # if the url is from WFS, then use WFSDataLoader
        # use a separate process
        # update the data status in the database
        pass