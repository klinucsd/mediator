import logging

import geopandas as gpd
from sqlalchemy import create_engine

from src.data_loader.data_loader import DataLoader
from src.db.mediator_db import db

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s UTC [%(process)d] %(levelname)s Data Loader %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

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
        # db.save_fake_data(self.table_name)

        engine = create_engine('postgresql://postgres:password@localhost:5432/wfr_datahub')

        # Read WFS data into a GeoDataFrame
        logging.info(f"Fetching data: {self.url}")
        gdf = gpd.read_file(self.url)

        # Write the GeoDataFrame to PostGIS
        logging.info(f"Saving data: {self.url}")
        gdf.to_postgis(self.table_name, engine, if_exists='replace', index=False)

        logging.info(f"Done with data: {self.url}")

        # Update the status
        db.update_data_status(self.url, 'Saved')
