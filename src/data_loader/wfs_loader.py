from src.data_loader.data_loader import DataLoader
from src.db.mediator_db import db

import urllib.parse
import urllib.request
import warnings
import xmltodict
import subprocess
from decouple import config
import json
import geopandas as gpd
from sqlalchemy import create_engine
from src.query_rewriter.rewrite_query import rewrite_query


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
        if "/wfs?" in url:
            layer_name = [x for x in url.split('&') if x.startswith('typeNames=')][0]
            baseUrl = url.split('?')[0]

            capabilities_url = baseUrl + '?service=WFS&version=2.0.1&request=GetCapabilities&typeNames=' + layer_name

            j = urllib.request.urlopen(capabilities_url)
            js_str = json.dumps(xmltodict.parse(j))
            js = json.loads(js_str)

            return 'error' not in js
        else:
            return False


    def load(self):
        """
        Load data served by WFS into the database and update the data status.

        This method loads data into the specified table and then updates the status
        of the data associated with the URL to 'Saved' in the mediator's data status table.
        If there was an error processing the data, the data status will be deleted in the
        data status table.
        """

        # Load data to self.table_name
        error = self.save_data(self.url, self.table_name)        

       # Update the status
        # if an error is returned, delete data status from data status table
        # if no error, update data status to 'Saved'
        if error:
            db.remove_data_status(self.url)  
            rewrite_query(self.username, f"SELECT md_mediator_error('{error}')", False)
        else:
            db.update_data_status(self.url, 'Saved')


    def save_data(self, url, table_name):

        layer_name = [x for x in url.split('&') if x.startswith('typeNames=')][0]
        baseUrl = url.split('?')[0]

        capabilities_url = baseUrl + '?service=WFS&version=2.0.1&request=GetCapabilities&typeNames=' + layer_name

        j = urllib.request.urlopen(capabilities_url)
        js_str = json.dumps(xmltodict.parse(j))
        js = json.loads(js_str)

        capabilities = js['wfs:WFS_Capabilities']['ows:OperationsMetadata']['ows:Operation']

        if any([e["@name"] == 'GetFeature' for e in capabilities]):

            getFeatures_url = baseUrl + '?service=WFS&version=2.0.0&request=GetFeature&typeNames=' + layer_name + '&outputFormat=application/json'

            response = urllib.request.urlopen(getFeatures_url)
            features = response.read()
            json_features = json.loads(features)
            
            gdf = gpd.GeoDataFrame.from_features(json_features)

            conn = db.connection_pool.getconn 
            engine = create_engine('postgresql://', creator=conn)            
            
            gdf.to_postgis(
                con=engine,
                schema='public',
                if_exists='replace',
                name=table_name,
            )

        else:
            print('Raster does not have proper capabilities for loading. GetCoverage is required')