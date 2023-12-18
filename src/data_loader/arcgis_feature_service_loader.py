import time

from src.data_loader.data_loader import DataLoader
from src.db.mediator_db import db


class ArcGISFeatureServiceLoader(DataLoader):

    @staticmethod
    def get_name() -> str:
        return 'ArcGIS Feature Service Loader'

    @staticmethod
    def get_description() -> str:
        return 'ArcGIS Feature Service Loader'

    @staticmethod
    def validate(url):
        """
        Check if this data loader is able to process the data at the URL.

        Parameters:
        - url (str): The URL to be validated.

        Returns:
        - bool: True if the loader can process the data, False otherwise.
        """
        return "FeatureServer" in url

    def load(self):
        """
        Load features served by ArcGIS into the database and update the data status.

        This method loads data into the specified table and then updates the status
        of the data associated with the URL to 'Saved' in the mediator's data status table.
        """

        # Load data to self.table_name
        db.save_fake_data(self.table_name)

        # Update the status
        db.update_data_status(self.url, 'Saved')
