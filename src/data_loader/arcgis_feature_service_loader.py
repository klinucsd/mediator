import time

from src.data_loader.data_loader import DataLoader
from src.db.mediator_db import db

import json
import requests
import urllib.parse
import urllib.request
import warnings

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

import multiprocessing
import multiprocess
import xml.etree.ElementTree as ET
from src.query_rewriter.rewrite_query import rewrite_query



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
        if "FeatureServer" in url:
            url_string = url + "?f=json"
            
            # Get object ids of features
            where = "1=1"
            url_string = url + "/query?where={}&returnIdsOnly=true&f=json".format(where)
            j = urllib.request.urlopen(url_string)
            js = json.load(j)

            return "error" not in js
        else:
            return False

    def load(self):
        """
        Load data served by ArcGIS Image Service into the database and update the data status.

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
        """
        Load data into PostGIS by fetching in batches. 
        Batches are set by maxRecordCount and number of object ids.

        Per batch, fetch objects within the batch range and parse together. 
        
        Use GeoPandas to_postgis function to post dataframe to postgis.

        Args:
            url (str) : URL to data
            table_name (str) : hashed table name

        Return: 
            If error has returned from the to_postgis command, return error string.
            If no error, return None 
        """

        # Get record extract limit
        url_string = url + "?f=json"
        j = urllib.request.urlopen(url_string)
        js = json.load(j)
        max_records_count = int(js["maxRecordCount"])
        max_records_count = min(max_records_count, 800)
        
        # Get object ids of features
        fields = "*"
        where = "1=1"
        url_string = url + "/query?where={}&returnIdsOnly=true&f=json".format(where)
        j = urllib.request.urlopen(url_string)
        js = json.load(j)
        id_field = js["objectIdFieldName"]
        id_list = js["objectIds"]
        id_list.sort()
        num_of_records = len(id_list)

        features_list = []

        if num_of_records > 0:

            def load_features(urlstring, return_dict):
                succeed = False
                while not succeed:
                    try:
                        resp = requests.get(urlstring, verify=False)
                        data = resp.json()
                        gdf = gpd.GeoDataFrame.from_features(data['features'], crs='EPSG:4269')
                        gdf = gdf.loc[gdf['geometry'].is_valid, :]
                        return_dict[urlstring] = gdf
                        succeed = True
                    except:
                        return 'Failed to load {}'.format(urlstring)

                    
            processes = []
            manager = multiprocessing.Manager()
            return_dict = manager.dict()
            
            request_number = 0;
            for i in range(0, num_of_records, max_records_count):

                request_number += 1

                to_rec = i + (max_records_count - 1)
                if to_rec > num_of_records:
                    to_rec = num_of_records - 1
                from_id = id_list[i]
                to_id = id_list[to_rec]
                where = "{} >= {} and {} <= {}".format(id_field, from_id, id_field, to_id)
                # print("  {}: {}".format(request_number, where))
                url_string = url + "/query?where={}&returnGeometry=true&outFields={}&f=geojson".format(where, fields)

                p = multiprocess.Process(target=load_features, args=[url_string, return_dict])
                p.start()
                processes.append(p)

            for p in processes:
                p.join()

            p.close()

            for url in return_dict.keys():
                features_list.append(return_dict[url])
            final_gdf = pd.concat(features_list)

            conn = db.connection_pool.getconn 
            engine = create_engine('postgresql://', creator=conn)

            try: 
                final_gdf.to_postgis(
                    con=engine,
                    schema='public',
                    if_exists='replace',
                    name=table_name,
                )

                return None
            except Exception as e:
                return e
            
            
        else:
            return "No records available to report"
