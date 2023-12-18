from src.data_loader.data_loader import DataLoader
from src.db.mediator_db import db


class WFSLoader(DataLoader):

    @staticmethod
    def get_name() -> str:
        return 'GeoServer WFS Loader'

    @staticmethod
    def get_description() -> str:
        return 'GeoServer WFS Loader'

    @staticmethod
    def validate(url):
        """
        Check if this data loader is able to process the data at the URL.

        Parameters:
        - url (str): The URL to be validated.

        Returns:
        - bool: True if the loader can process the data, False otherwise.
        """
        return "/wfs?" in url

    def load(self):
        """
        Load data served by WFS into the database and update the data status.

        This method loads data into the specified table and then updates the status
        of the data associated with the URL to 'Saved' in the mediator's data status table.
        """

        # Load data to self.table_name
        db.save_fake_data(self.table_name)

        # Update the status
        db.update_data_status(self.url, 'Saved')
