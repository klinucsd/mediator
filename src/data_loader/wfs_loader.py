import json
import logging
import subprocess
import tempfile
from multiprocessing import Event, Process
from urllib.parse import urlparse, parse_qs, parse_qsl, ParseResult, urlencode
from xml.etree.ElementTree import fromstring

import geopandas
import pyproj
import requests
from decouple import config
from owslib.wfs import WebFeatureService
from sqlalchemy import create_engine, NullPool

from src.data_loader.data_loader import DataLoader, DataLoaderError

DATA_LOAD_MAX_PROCESSES = config('data_load_max_processes', cast=int)
DATA_LOAD_FEATURES_PER_PROCESS = config('data_load_features_per_process', cast=int)
DATA_LOAD_RETRIES_ON_ERROR = config('data_load_retries_on_error', cast=int)
DATA_LOAD_INIT_FEATURES = config('data_load_init_features', cast=int)


# This WFS loader is designed with two key objectives:
#
# Resource Efficiency: To mitigate the risk of excessive memory usage and prevent potential system crashes when
# dealing with large WFS datasets. The loader ensures that only manageable portions of data are read at a time,
# preventing resource exhaustion.
#
# Concurrent Loading: To enhance overall loading speed, the loader employs a concurrent approach. It reads and
# processes smaller chunks of WFS data simultaneously, allowing for faster and more efficient loading of the entire
# dataset. This concurrent strategy optimizes resource utilization and reduces the overall loading time.

def save_gml_to_db(gml_binary, table_name, mode):
    with tempfile.NamedTemporaryFile(suffix=".gml", mode="wb") as temp:
        temp.write(gml_binary)
        temp.flush()  # Ensure data is written to the file

        command = [
            "ogr2ogr",
            f"-{mode}",
            "-f", "PostgreSQL",
            f"PG:dbname={config('db_name')} host={config('db_host')} port={config('db_port')} user={config('db_user')} password=password",
            temp.name,
            "-nln", table_name
        ]
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            # print(result.stdout)  # Print successful output
        except subprocess.CalledProcessError as error:
            # print("Error:", error)
            # print("Command output:", error.output)  # Access error out
            raise DataLoaderError(f'Error when saving GML to PostGIS: {error}')


# This function is used by a new spawned process to save WFS_LOAD_FEATURES_PER_PROCESS
# features starting from start_index to PostGIS
def process_load_features(self_url, base_url, version, type_name, epsg_code, start_index,
                          sort_by, table_name, output_format, vendor, error_event):
    """
        Load features from a Web Feature Service (WFS) into a PostgresSQL/PostGIS database.

        Args:
            self_url (str): The original URL for error logging
            base_url (str): The base URL of the WFS service.
            version (str): The version of the WFS service (e.g., '1.1.0', '2.0.0').
            type_name (str): The name of the feature type to retrieve from the WFS.
            epsg_code (str): The EPSG code representing the coordinate reference system (CRS).
            start_index (int): The starting index for retrieving features in a paginated manner.
            sort_by (str): The attribute to use for sorting features.
            table_name (str): The name of the PostgresSQL table to store the features.
            output_format (str): The name for the JSON output format.
            vendor (str): The name of the server vendor.
            error_event (threading.Event): An event to signal if an error occurs during the data loading process.

        Raises:
            DataLoaderError: If the maximum number of retries is reached and the data loading process fails.

        Note:
            This function attempts to load features from the specified WFS service into a PostgreSQL/PostGIS database.
            It uses pagination to load features in chunks and retries in case of errors.

        Example:
            process_load_features(
                url='https://example.com/wfs',
                version='2.0.0',
                type_name='roads',
                epsg_code='4326',
                start_index=0,
                sort_by='objectid',
                table_name='roads_table',
                output_format='GEOJSON',
                vendor='ArcGIS',
                error_event=my_error_event
            )
    """
    # Set the number of retries in case of an error during loading
    tries = 0

    # Retry loading features in case of an error or no error
    while tries < DATA_LOAD_RETRIES_ON_ERROR:
        try:
            # Create a WebFeatureService instance
            wfs = WebFeatureService(base_url, version, timeout=120)

            # Make a GetFeature request to the WFS service
            response = wfs.getfeature(typename=type_name,
                                      outputFormat=output_format,
                                      startindex=start_index,
                                      sortby=sort_by,
                                      maxfeatures=DATA_LOAD_FEATURES_PER_PROCESS)

            data = response.read()
            if 'json' in output_format.lower():
                # Load the JSON features from the response
                json_features = json.loads(data)

                # Sometimes the server returns an empty feature set
                if len(json_features['features']) == 0:
                    # Log the successful loading of features
                    logging.info(
                        f"Loaded from {start_index} To {start_index + DATA_LOAD_FEATURES_PER_PROCESS}: {base_url}: {type_name}")
                    # Return after successful loading
                    return

                # Create a GeoDataFrame from the JSON features with the specified CRS
                crs = pyproj.CRS.from_epsg(int(epsg_code))
                gdf = geopandas.GeoDataFrame.from_features(json_features, crs=crs)

                try:
                    # Construct the PostgreSQL connection URL
                    postgres_url = f"postgresql://{config('db_user')}:{config('db_password')}@{config('db_host')}:{config('db_port')}/{config('db_name')}"

                    # Create the SQLAlchemy engine
                    engine = create_engine(postgres_url, poolclass=NullPool)

                    # Use the connection for GeoDataFrame.to_postgis
                    gdf.to_postgis(name=table_name, con=engine, schema='public', if_exists='append')

                    # Explicitly close the engine
                    engine.dispose()
                except Exception as e:
                    # Handle any errors that occur during saving to PostGIS
                    logging.error(f"Error saving to PostGIS: {e}")
                    error_event.set()
                    DataLoader.set_loading_error(self_url,
                                                 f"Failed saving to PostGIS from {start_index} To {start_index + DATA_LOAD_FEATURES_PER_PROCESS}: {base_url}: {type_name}: {e}")
                    return
            elif 'gml' in output_format.lower():
                # gdf = geopandas.read_file(StringIO(data.decode('utf-8')), driver='GML')
                save_gml_to_db(data, table_name, 'append')

            # Log the successful loading of features
            logging.info(
                f"Loaded from {start_index} To {start_index + DATA_LOAD_FEATURES_PER_PROCESS}: {base_url}: {type_name}")

            # Return after successful loading
            return
        except Exception as e:
            # traceback.print_exc()
            # Log the retry attempt in case of an error
            logging.info(
                f"Try loading again from {start_index} To {start_index + DATA_LOAD_FEATURES_PER_PROCESS}: {base_url}: {type_name}: {tries}: {e}")
            tries += 1

    # If all retries fail, set the error event and raise an exception
    error_event.set()
    DataLoader.set_loading_error(self_url,
                                 f"Failed loading from {start_index} To {start_index + DATA_LOAD_FEATURES_PER_PROCESS}: {base_url}: {type_name}")
    logging.info(
        f"Failed loading from {start_index} To {start_index + DATA_LOAD_FEATURES_PER_PROCESS}: {base_url}: {type_name}")


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
        try:
            # Parse the url and check the query parameter typename exists and is valid
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            query_params = {key.lower(): value for key, value in query_params.items()}
            if 'typename' in query_params.keys():
                typename = query_params['typename'][0]
                wfs = WebFeatureService(url, "1.1.0")  # we will support if only 1.1.0 is implemented
                layers = wfs.contents.keys()

                # Check if the layer with the typename exists
                # MapServer WFS may not have a name space with a layer
                return typename in layers or typename.split(':')[-1] in layers
        except requests.exceptions.HTTPError as e:
            return False

    # This method is not used yet.
    # The purpose of this method is to standardize an url to avoid duplicate loading
    def normalize_url(self):
        parsed = urlparse(self.url)
        query = sorted([(key.lower(), value) for key, value in parse_qsl(parsed.query)])
        # this WFS loader only needs a typename; other parameters will be generated based on the capabilities.
        query = [(key, value) for key, value in query if key == 'typename']
        parsed = ParseResult(parsed.scheme,
                             parsed.netloc,
                             parsed.path,
                             parsed.params,
                             urlencode(query),
                             parsed.fragment)
        return parsed.geturl()

    @staticmethod
    def __get_sort_by(fields):
        id_fields = [key for key, value in fields.items() if
                     key.lower().endswith('id') and (value == 'long' or value == 'int' or value == 'double')]
        if id_fields:
            return id_fields[0]

        string_id_fields = [key for key, value in fields.items() if key.lower().endswith('id') and value == 'string']
        if string_id_fields:
            return string_id_fields[0]

        return list(fields.keys())[0]

    @staticmethod
    def __get_total_feature_count(base_url, typename, version):
        response = requests.get(base_url, params={
            'service': 'WFS',
            'version': version,
            'request': 'GetFeature',
            'resultType': 'hits',
            'typename': typename
        })
        hits_xml = fromstring(response.content)
        if 'numberOfFeatures' in hits_xml.attrib:
            try:
                return int(fromstring(response.content).attrib['numberOfFeatures'])
            except Exception:
                pass

        if 'numberMatched' in hits_xml.attrib:
            try:
                return int(fromstring(response.content).attrib['numberMatched'])
            except Exception:
                pass

        if 'numberReturned' in hits_xml.attrib:
            try:
                return int(fromstring(response.content).attrib['numberReturned'])
            except Exception:
                pass

        raise DataLoaderError('Could not find the total feature number.')

    @staticmethod
    def __detect_server_vendor(wfs):
        try:
            response = wfs.getcapabilities()
            capabilities = str(response.read())

            # Check for GeoServer-specific namespace
            if 'geoserver' in capabilities:
                return 'GeoServer'

            # Check for MapServer-specific identifier
            if 'mapserver' in capabilities:
                return 'MapServer'

            # Check for ArcGIS-specific identifier
            if 'arcgis' in capabilities:
                return 'ArcGIS'

            return 'Unknown'

        except Exception as e:
            print(f"Checking server vendor error: {e}")
            return 'Unknown'

    def load(self):
        """
        Load data served by WFS into the database and update the data status.

        This method loads data into the specified table and then updates the status
        of the data associated with the URL to 'Saved' in the mediator's data status table.
        """
        # Get base url
        parsed_url = urlparse(self.url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

        # Get all query parameters
        query_params = parse_qs(parsed_url.query)
        query_params = {key.lower(): value for key, value in query_params.items()}

        # Check if the query parameter typename occurs
        if 'typename' in query_params.keys():
            typename = query_params['typename'][0]
        else:
            raise DataLoaderError(f'Missing the parameter typename')

        # Get the version of this WFS
        wfs = WebFeatureService(base_url, '1.1.0', timeout=120)
        version = wfs.identification.version
        # logging.info(wfs.identification.__dict__)

        # Get the server vendor
        vendor = self.__detect_server_vendor(wfs)
        logging.info(f"Vendor: {vendor}")

        # Get all layers
        layers = wfs.contents.keys()
        logging.info(layers)

        # Check if the layer with the typename exists
        # MapServer WFS may not have a name space in the layers
        if typename not in layers and typename.split(':')[-1] not in layers:
            raise DataLoaderError(f'{typename} not found')

        # Get the schema of the typename
        sort_by = None
        if vendor == 'GeoServer' or vendor == 'Unknown':
            schema = wfs.get_schema(typename)
            logging.info(f"schema: {schema['properties']}")
            sort_by = self.__get_sort_by(schema['properties'])
            logging.info(f"sort_by: {sort_by}")

        # Starting index for all the features
        start_index = 0

        # check the allowed output formats for GetFeature and choose json or gml if exists
        output_format = 'application/json'
        get_feature = wfs.getOperationByName("GetFeature")
        output_formats = get_feature.parameters['outputFormat']['values']
        logging.info(f"allowed output formats: {output_formats}")
        json_formats = [fmt for fmt in output_formats if 'json' in fmt.lower()]
        json_formats.sort(key=len)
        if json_formats:
            output_format = json_formats[0]
        else:
            gml_formats = [fmt for fmt in output_formats if 'gml' in fmt.lower()]
            if gml_formats:
                output_format = gml_formats[0]
        logging.info(f"output format: {output_format}")

        # Special setting for ArcGIS
        if vendor == 'ArcGIS':
            # for ArcGIS WFS, GetFeature supports geojson when using the version 2.0.0,
            # even it is not specified in GetCapabilities
            wfs = WebFeatureService(base_url, '2.0.0', timeout=60)
            version = '2.0.0'
            output_format = 'geojson'
            logging.info(f"change the output format to {output_format} for ArcGIS")

        # Get the total feature number
        # Note: found several cases in which the actual feature number is smaller than
        # the declared total feature number
        total = self.__get_total_feature_count(base_url, typename, version)

        # Get the projection. Sometimes returned features may not associate with an epsg code.
        epsg_code = wfs.contents[typename].crsOptions[0].code
        crs = pyproj.CRS.from_epsg(int(epsg_code))

        # Fetch the initial features
        response = wfs.getfeature(typename=typename,
                                  outputFormat=output_format,
                                  startindex=start_index,
                                  sortby=sort_by,
                                  maxfeatures=DATA_LOAD_INIT_FEATURES)
        data = response.read()
        if 'json' in output_format.lower():
            # Create a GeoDataFrame from the JSON features with the specified CRS
            data_json = json.loads(data)
            gdf = geopandas.GeoDataFrame.from_features(data_json, crs=crs)

            # Construct the PostgresSQL connection URL
            postgres_url = f"postgresql://{config('db_user')}:{config('db_password')}@{config('db_host')}:{config('db_port')}/{config('db_name')}"

            # Create the SQLAlchemy engine.
            # Note: using NullPool to prevent SQLAlchemy starting a connection pool.
            engine = create_engine(postgres_url, poolclass=NullPool)

            # Use the connection for GeoDataFrame.to_postgis.
            # Note: use 'replace' strategy to initialize the table
            gdf.to_postgis(name=self.table_name, con=engine, schema='public', if_exists='replace')

            # Explicitly close the engine
            engine.dispose()

        elif 'gml' in output_format.lower():
            # GeoPandas is very buggy to load GML; Use ogr2ogr instead
            # gdf = geopandas.read_file(StringIO(data.decode('utf-8')), driver='GML', crs=crs)
            save_gml_to_db(data, self.table_name, 'overwrite')

        logging.info(f"Loaded from {start_index} To {start_index + DATA_LOAD_INIT_FEATURES} out of {total}: {self.url}")

        # Create an event to signal an error by processes
        error_event = Event()

        # Start more processes to save the next WFS_LOAD_FEATURES_PER_PROCESS features
        start_index = start_index + DATA_LOAD_INIT_FEATURES;
        processes = []
        while total > start_index and not error_event.is_set():
            process = Process(target=process_load_features,
                              args=(self.url,
                                    base_url,
                                    version,
                                    typename,
                                    epsg_code,
                                    start_index,
                                    sort_by,
                                    self.table_name,
                                    output_format,
                                    vendor,
                                    error_event))
            processes.append(process)
            process.start()
            logging.info(
                f"Start loading {start_index} to {start_index + DATA_LOAD_FEATURES_PER_PROCESS} out of {total}: {self.url}")
            start_index = start_index + DATA_LOAD_FEATURES_PER_PROCESS

            # To be nice to the remote server and the mediator
            # Start limited new processes to load data
            if len(processes) == DATA_LOAD_MAX_PROCESSES:
                for process in processes:
                    process.join()

                # Clear the processes list
                processes.clear()

        # Complete the left processes
        if len(processes) > 0 and not error_event.is_set():
            for process in processes:
                process.join()

        # Check if an error occurred in any process
        if error_event.is_set():
            logging.info(f"An error occurred in one of the processes: {self.url}")
            return

        logging.info(f"Completed data loading: {self.url}")

        # Update the status
        DataLoader.update_data_status(self.url, 'Saved')
