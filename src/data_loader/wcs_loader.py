import logging
import subprocess
import tempfile
import xml
from urllib.parse import urlparse, parse_qs

import requests
from decouple import config
from owslib.wcs import WebCoverageService

from src.data_loader.data_loader import DataLoader, DataLoaderError


class WCSLoader(DataLoader):

    @staticmethod
    def get_name() -> str:
        return 'GeoServer WCS Loader'

    @staticmethod
    def get_description() -> str:
        return "This data loader is designed for storing publicly accessible WCS data locally through WCS " \
               "version 2.0.1 or above. It accommodates simplified WCS URLs, such as " \
               "https://wcs.foo.com?coverageid=mydata, and automatically supplements additional parameters " \
               "as needed during the access. "

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
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            query_params = {key.lower(): value for key, value in query_params.items()}
            if 'coverageid' in query_params.keys():
                coverage_id = query_params['coverageid'][0]
                wcs = WebCoverageService(url, "2.0.1")
                layers = wcs.contents.keys()
                return coverage_id in layers or coverage_id.split(':')[-1] in layers
            else:
                return False
        except requests.exceptions.HTTPError:
            return False

    @staticmethod
    def __detect_server_vendor(wcs):
        try:
            capabilities = xml.etree.ElementTree.tostring(wcs._capabilities).decode()

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
        Load data served by WCS into the database and update the data status.

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
        if 'coverageid' in query_params.keys():
            coverage_id = query_params['coverageid'][0]
        else:
            raise DataLoaderError(f'Missing the parameter coverageid')

        # Get the version of this WFS
        wcs = WebCoverageService(base_url, '2.0.1', timeout=120)
        version = wcs.identification.version
        logging.info(wcs.identification.__dict__)

        # Get the server vendor
        vendor = self.__detect_server_vendor(wcs)
        logging.info(f"Vendor: {vendor}")

        # Get all layers
        layers = wcs.contents.keys()
        logging.info(layers)

        # Check if the layer with the coverageid exists
        # MapServer WFS may not have a name space in the layers
        if coverage_id not in layers and coverage_id.split(':')[-1] not in layers:
            raise DataLoaderError(f'{coverage_id} not found')

        coverage = wcs.contents[coverage_id]

        # Get its bounding boxes and projection
        bounding_boxes = coverage.boundingboxes
        for bounding_box in bounding_boxes:
            projection = f"EPSG:{bounding_box['nativeSrs'].split('/')[-1]}"
            bbox = bounding_box['bbox']
            logging.info(f"projection: {projection}")
            logging.info(f"bbox: {bbox}")

        # Get other attributes for this coverage id
        axis_labels = coverage.grid.axislabels
        logging.info(f"axis_labels: {axis_labels}")

        # Get dimension
        dimension = coverage.grid.dimension
        logging.info(f"dimension: {dimension}")

        # Get grid lower and upper bounds
        low_limits = coverage.grid.lowlimits
        logging.info(f"low_limits: {low_limits}")

        # Get high limits
        high_limits = coverage.grid.highlimits
        logging.info(f"high_limits: {high_limits}")

        # Get offset vectors for geo axes
        offset_vectors = coverage.grid.offsetvectors
        logging.info(f"offset_vectors: {offset_vectors}")

        # For coverage with time axis get the date time values
        time_positions = coverage.timepositions
        logging.info(f"time_positions: {time_positions}")

        # Check if GeoTIFF is supported
        supported_formats = coverage.supportedFormats
        logging.info(f"Supported formats: {supported_formats}")
        tiff_formats = [item for item in supported_formats if 'tiff' in item.lower()]
        if tiff_formats:
            output_format = tiff_formats[0]
            logging.info(f'Output format: {output_format}')
        else:
            logging.info(f"Failed loading: {base_url}: {coverage_id}")
            DataLoader.set_loading_error(self.url, f"Failed loading: {base_url}: {coverage_id}")
            raise DataLoaderError(f"The GeoTIFF format is not supported for this coverage: {self.url}");

        # Download Data as GeoTIFF to a temporary file
        logging.info(f"Downloading: {self.url}")
        get_coverage = wcs.getCoverage(identifier=[coverage_id],
                                       bbox=bbox,
                                       format=output_format,
                                       crs=projection,
                                       width=int(high_limits[0]),
                                       height=int(high_limits[1]),
                                       timeout=120)
        logging.info(f"URL: {get_coverage.geturl()}")
        with tempfile.NamedTemporaryFile(suffix=".tif", mode="wb") as temp:
            temp.write(get_coverage.read())
            temp.flush()  # Ensure data is written to the file
            logging.info(f'Saved as a temp file at: {temp.name}')

            command = [
                'raster2pgsql',
                '-s',
                projection,
                '-M', '-C', '-I', '-F', '-t', '100x100',
                temp.name,
                f'public.{self.table_name}'
            ]
            try:
                logging.info(f'Saving to PostGIS: {command}')
                with subprocess.Popen(command, stdout=subprocess.PIPE) as raster2pgsql_process:
                    subprocess.run(["psql", "-q",
                                    "-h", f"{config('db_host')}",
                                    "-p", f"{config('db_port')}",
                                    "-U", f"{config('db_user')}",
                                    "-d", f"{config('db_name')}"],
                                   stdin=raster2pgsql_process.stdout,
                                   stdout=subprocess.DEVNULL)
            except subprocess.CalledProcessError as error:
                print("Error:", error)
                print("Command output:", error.output)  # Access error out
                raise DataLoaderError(f'Error when saving GeoTIFF to PostGIS: {error}')

        logging.info(f"Completed data loading: {self.url}")

        # Update the status
        DataLoader.update_data_status(self.url, 'Saved')
