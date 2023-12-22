from src.data_loader.data_loader import DataLoader
from src.db.mediator_db import db

import urllib.parse
import urllib.request
import warnings
import xmltodict
import subprocess
from decouple import config
import json
from src.query_rewriter.rewrite_query import rewrite_query
import os
import logging



class WCSLoader(DataLoader):

    @staticmethod
    def get_name() -> str:
        return 'GeoServer WCS Loader'

    @staticmethod
    def get_description() -> str:
        return 'GeoServer WCS Loader'

    @staticmethod
    def validate(url):
        """
        Check url for wcs call
        Perform another check for proper capabilities required to perform data loading in WCS
        """
        if "/wcs?" in url:
            layer_name = [x for x in url.split('&') if x.startswith('CoverageId=')][0][11:]
            baseUrl = url.split('?')[0]

            # check to make sure the server has the proper capabilities
            capabilities_url = baseUrl + '?service=WCS&version=2.0.1&request=GetCapabilities&CoverageId=' + layer_name

            j = urllib.request.urlopen(capabilities_url)
            js_str = json.dumps(xmltodict.parse(j))
            js = json.loads(js_str)
            
            capabilities = js['wcs:Capabilities']['ows:OperationsMetadata']['ows:Operation']

            has_Capabilities = any([e["@name"] == 'GetCoverage' for e in capabilities]) \
                and any([e["@name"] == 'DescribeCoverage' for e in capabilities])
        
            return has_Capabilities
        
        return False

    def load(self):
        """
        Load data served by WCS into the database and update the data status.

        This method loads data into the specified table and then updates the status
        of the data associated with the URL to 'Saved' in the mediator's data status table.
        If there was an error processing the data, the data status will be deleted in the
        data status table.
        """

        conn_str = db.get_conn_str()

        # Load data to self.table_name
        error = self.save_data(conn_str, self.url, self.table_name)

        # Update the status
        # if an error is returned, delete data status from data status table
        # if no error, update data status to 'Saved'
        if error:
            db.remove_data_status(self.url)    
            rewrite_query(self.username, f"SELECT md_mediator_error('{error}')", False)
        else:
            db.update_data_status(self.url, 'Saved')


    def save_data(self, conn_str, url, table_name):
        """
        Fetch coverage data through GetCoverage function. DescribeCoverage is performed
        to fetch required fields for GetCoverage.
         
        Once fetched, save image to temp file location. Perform raster2pgsql to convert
        image to rast fields on postgis db.

        Args:
            conn_str (str) : Connection String to db
            url (str) : URL to data
            table_name (str) : hashed table name

        Return: 
            If error has returned from the raster2pgsql command, return error string.
            If no error, return None 
        """
        layer_name = [x for x in url.split('&') if x.startswith('CoverageId=')][0][11:]
        baseUrl = url.split('?')[0]

        # check to make sure the server has the proper capabilities
        capabilities_url = baseUrl + '?service=WCS&version=2.0.1&request=GetCapabilities&CoverageId=' + layer_name

        j = urllib.request.urlopen(capabilities_url)
        js_str = json.dumps(xmltodict.parse(j))
        js = json.loads(js_str)
        
        capabilities = js['wcs:Capabilities']['ows:OperationsMetadata']['ows:Operation']

        has_Capabilities = any([e["@name"] == 'GetCoverage' for e in capabilities]) \
            and any([e["@name"] == 'DescribeCoverage' for e in capabilities])

        if has_Capabilities:

            # describe coverage to extract crs for raster2pgsql command
            describe_url = baseUrl + '?service=WCS&version=2.0.1&request=DescribeCoverage&CoverageId=' + layer_name
            
            j = urllib.request.urlopen(describe_url)
            js_str = json.dumps(xmltodict.parse(j))
            js = json.loads(js_str)

            crs = js['wcs:CoverageDescriptions']['wcs:CoverageDescription']['gml:boundedBy']['gml:Envelope']['@srsName'].split('/')[-1]

            # get coverage and load into tmp location for raster2pgsql 
            getCoverage_url = baseUrl + '?service=WCS&version=2.0.1&request=GetCoverage&CoverageId=' + layer_name + '&format=image/geotiff'
            coverage = urllib.request.urlopen(getCoverage_url)

            file_loc = f'{config("tmp_load_data_file_loc")}{layer_name}.tif'

            logging.info('is dir')
            logging.info(os.path.isdir(config("tmp_load_data_file_loc")))

            with open(file_loc, 'wb') as file:
                file.write(coverage.read())

            # pipe processes together with subprocess
            # if error, return message
            subprocess_cmd = ['raster2pgsql', '-s', str(crs), '-c','-C','-I','-F','-t','100x100', file_loc, f'public.{table_name}']
            p = subprocess.Popen(subprocess_cmd,stdout=subprocess.PIPE)
            command2 = ["psql", conn_str]
            try: 
                result = subprocess.run(command2, stdin=p.stdout, capture_output=True)
                error = result.stderr.decode("utf-8")

                subprocess.Popen(['rm', file_loc])

                if "ERROR" in error:
                    return error
                else:
                    return None
            except subprocess.CalledProcessError as e:
                return e

        else:
            print('Raster does not have proper capabilities for loading. GetCoverage is required')

    