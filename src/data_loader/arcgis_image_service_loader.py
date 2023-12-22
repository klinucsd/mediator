
from src.data_loader.data_loader import DataLoader
from src.db.mediator_db import db

import json
import os

import requests
import urllib.parse
import urllib.request
import warnings
import subprocess
from decouple import config
from src.query_rewriter.rewrite_query import rewrite_query



# remove warnings
# warnings.filterwarnings('ignore')

class ArcGISImageServiceLoader(DataLoader):

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

        if "ImageServer" in url:
            capabilities_url = url + '/WCSServer?request=GetCapabilities&service=WCS&f=json'
            j = urllib.request.urlopen(capabilities_url)
            js = json.load(j)
            return 'error' not in js
        else:
            return False
    
    def load(self):

        """
        Load data served by ArcGIS Feature Service into the database and update the data status.

        This method loads data into the specified table and then updates the status
        of the data associated with the URL to 'Saved' in the mediator's data status table.
        If there was an error processing the data, the data status will be deleted in the
        data status table.
        """

        conn_str = db.get_conn_str()

        # Load from image service
        error = self.save_data(self.url, self.table_name, conn_str)

        # Update the status
        # if an error is returned, delete data status from data status table
        # if no error, update data status to 'Saved'
        if error:
            db.remove_data_status(self.url)    
            rewrite_query(self.username, f"SELECT md_mediator_error('{error}')", False)
        else:
            db.update_data_status(self.url, 'Saved')


    def save_data(self, url, table_name, conn_str):
        """
        Fetch image data through exportImport function.

        Once fetched, save image to temp file location. Perform raster2pgsql to convert
        image to rast fields on postgis db.

        Args:
            url (str) : URL to data
            table_name (str) : hashed table name
            conn_str (str) : Connection String to db

        Return: 
            If error has returned from the raster2pgsql command, return error string.
            If no error, return None 
        """

        # get capabilities with wcs to construct correct exportImage url
        capabilities_url = url + '/WCSServer?request=GetCapabilities&service=WCS&f=json'
        j = urllib.request.urlopen(capabilities_url)
        js = json.load(j)

        if 'error' in js:
            return 'Querying capabilities not allowed'
        else:
            # get image information
            xmin = js['extent']['xmin']
            ymin = js['extent']['ymin']
            xmax = js['extent']['xmax']
            ymax = js['extent']['ymax']
            crs = js['extent']['spatialReference']['latestWkid']
            pixelType = js['pixelType']

            # load exportImage
            image_url = url + f'/exportImage?bbox={xmin},{ymin},{xmax},{ymax}&format=tiff&pixelType={pixelType}&f=pjson'
            j = urllib.request.urlopen(image_url)
            js = json.load(j)

            image = requests.get(js['href']).content

            layer_name_str = '_'.join(url.split('/')[:4])

            layer_name = '_'.join(e for e in layer_name_str if e.isalnum())

            file_loc = f'{config("tmp_load_data_file_loc")}{layer_name}.tif'

            # save image to file to be used in the raster2pgsql
            with open(file_loc, 'wb') as file:
                file.write(image)
            
            # create command with pipes
            # subprocess allows for error retrieval from command results
            subprocess_cmd = ['raster2pgsql', '-s', str(crs), '-c','-C','-I','-F','-t','100x100', file_loc, f'public.{table_name}']
            p = subprocess.Popen(subprocess_cmd,stdout=subprocess.PIPE)
            command2 = ["psql", conn_str]
            try: 
                result = subprocess.run(command2, stdin=p.stdout, capture_output=True)
                error = result.stderr.decode("utf-8")

                if "ERROR" in error:
                    return error
                else:
                    return None
            except subprocess.CalledProcessError as e:
                return e
