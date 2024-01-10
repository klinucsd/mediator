import concurrent
import logging
import time
from concurrent.futures import ProcessPoolExecutor

import geopandas
import numpy
import requests
from arcgis.auth.api import urllib3
from arcgis.features import FeatureLayer
from decouple import config
from sqlalchemy import create_engine, NullPool

from src.data_loader.data_loader import DataLoader, DataLoaderError

DATA_LOAD_RETRIES_ON_ERROR = config('data_load_retries_on_error', cast=int)


def load_features(self_url, table_name, where, wkid, schema):
    logging.info(f"Loading by query: {where}: {self_url}")

    # Set the number of retries in case of an error during loading
    tries = 0

    # Disable SSL warnings for this request
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Retry loading features in case of an error or no error
    while tries < DATA_LOAD_RETRIES_ON_ERROR:
        try:
            url_string = self_url + "/query?where={}&returnGeometry=true&outFields={}&f=geojson".format(where, '*')
            resp = requests.get(url_string, verify=False)
            data = resp.json()

            gdf = geopandas.GeoDataFrame.from_features(data['features'], crs=f'EPSG:{wkid}')
            gdf = gdf.loc[gdf['geometry'].is_valid, :]

            gdf.replace([numpy.nan, numpy.inf, -numpy.inf], 0, inplace=True)
            for field in schema:
                if field['type'] == 'esriFieldTypeInteger':
                    gdf[field['name']] = gdf[field['name']].astype('int64')
                elif field['type'] == 'esriFieldTypeSmallInteger':
                    gdf[field['name']] = gdf[field['name']].astype('int64')
                elif field['type'] == 'esriFieldTypeString':
                    gdf[field['name']] = gdf[field['name']].astype('str')

            # Construct the PostgreSQL connection URL
            postgres_url = f"postgresql://{config('db_user')}:{config('db_password')}@{config('db_host')}:{config('db_port')}/{config('db_name')}"

            # Create the SQLAlchemy engine
            engine = create_engine(postgres_url, poolclass=NullPool)

            # Use the connection for GeoDataFrame.to_postgis
            gdf.to_postgis(name=table_name, con=engine, schema='public', if_exists='append')

            # Explicitly close the engine
            engine.dispose()

            # Log the successful loading of features
            logging.info(f"Done the query: {where}: {self_url}")
            return where
        except Exception as e:
            # Log the retry attempt in case of an error
            logging.info(f"Try loading by query again: {where}: {self_url}: {tries}: {e}")
            tries += 1

    # If all retries fail, set the error event and raise an exception
    logging.info(f"Failed loading by query: {where}: {self_url}")
    DataLoader.set_loading_error(self_url, f"Failed loading by query: {where}")
    raise DataLoaderError(f"Failed loading by query: {where}: {self_url}")


class ArcGISFeatureServiceLoader(DataLoader):

    @staticmethod
    def get_name() -> str:
        return 'ArcGIS Feature Service Loader'

    @staticmethod
    def get_description() -> str:
        return 'This data loader is designed for storing publicly accessible ArcGIS Feature Service data locally.'

    @staticmethod
    def validate(url):
        """
        Check if this data loader is able to process the data at the URL.

        Parameters:
        - url (str): The URL to be validated.

        Returns:
        - bool: True if the loader can process the data, False otherwise.
        """
        try:
            if "/FeatureServer/" in url:
                layer = FeatureLayer(url)
                extent = layer.properties.extent
                return extent is not None
            else:
                return False
        except Exception:
            return False

    def load(self):
        """
        Load features served by ArcGIS into the database and update the data status.

        This method loads data into the specified table and then updates the status
        of the data associated with the URL to 'Saved' in the mediator's data status table.
        """
        # Create a FeatureLayer by fetching its metadata
        layer = FeatureLayer(self.url)
        extent = layer.properties.extent
        logging.info(f"Extent: {extent}")

        # Get the spatial reference (projection) of the feature layer
        wkid = layer.properties.extent.spatialReference.wkid
        logging.info(f"Projection: {wkid}")

        # Gets the maximum record count of the layer
        max_record_count = layer.properties.maxRecordCount
        logging.info(f"maxRecordCount: {max_record_count}")

        # Get the geometry type of the feature layer
        geometry_type = layer.properties.geometryType
        logging.info(f"Geometry Type: {geometry_type}")

        # Get the schema information
        schema = layer.properties.fields
        for field in schema:
            logging.info(f"Field Name: {field['name']}, Type: {field['type']}")

        # Get objectIds of all the features
        result = layer.query(where="1=1", return_ids_only=True)
        id_field_name = result["objectIdFieldName"]
        object_ids = result["objectIds"]
        object_ids.sort()
        total_record_count = len(object_ids)
        logging.info(f"Loaded {total_record_count} objectIds")

        # create a process pool with the default number of worker processes
        executor = ProcessPoolExecutor()

        # Check the pool size
        current_pool_size = executor._max_workers
        logging.info(f"current_pool_size: {current_pool_size}")

        available_slots = executor._max_workers - len(executor._processes)
        logging.info(f"available_slots: {available_slots}")

        futures = []
        for i in range(0, total_record_count, max_record_count):

            # Set up a where condition for this iteration
            to_rec = i + (max_record_count - 1)
            if to_rec > total_record_count:
                to_rec = total_record_count - 1
            from_id = object_ids[i]
            to_id = object_ids[to_rec]
            where = "{} >= {} and {} <= {}".format(id_field_name, from_id, id_field_name, to_id)

            available_slots = executor._max_workers - len(executor._processes)
            logging.info(f"--- Processing: {where}: available_slots: {available_slots} ---")

            logging.info(f"Submitting: {where}")
            future = executor.submit(load_features, self.url, self.table_name, where, wkid, schema)
            futures.append(future)

            if i == 0:
                time.sleep(5)

        # Wait for all tasks to complete
        done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
        for future in done:
            # Process errors
            if future.exception():
                executor.shutdown()
                concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
                DataLoader.set_loading_error(self.url, f'Failed downloading data: {future.exception()}')
                logging.info(f'Failed fetching data: {future.exception()}')
                return

        logging.info(f"Completed data loading: {self.url}")

        # Update the status
        DataLoader.update_data_status(self.url, 'Saved')
