####### setup
from typing import Union, List, Optional
import logging
import warnings
import requests
import pandas as pd
import getpass
import zipfile
import io
import os
import random
import concurrent.futures
from datetime import datetime
from datetime import timedelta
from enum import Enum
import plotly.express as px
from tcia_utils.utils import searchDf
from tcia_utils.utils import copy_df_cols
from tcia_utils.utils import format_disk_space
from tcia_utils.utils import remove_html_tags
from tcia_utils.datacite import getDoi
from IPython.display import HTML

class StopExecution(Exception):
    def _render_traceback_(self):
        pass


_log = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s'
    , level=logging.INFO
)


def log_request_exception(err: requests.exceptions.RequestException) -> None:
    """
    Logs details of a RequestException, categorizing the error type.

    Args:
        err (requests.exceptions.RequestException): The exception raised during a request.

    Returns:
        None
    """
    if isinstance(err, requests.exceptions.HTTPError):
        if err.response is not None:
            status_code = err.response.status_code
            if status_code == 401:
                _log.error(f"Authentication Error: {err}. Status code: 401. Unauthorized access.")
            elif status_code == 403:
                _log.error(f"Permission Error: {err}. Status code: 403. Forbidden access.")
            elif status_code == 404:
                _log.error(f"Resource Not Found: {err}. Status code: 404.")
            else:
                _log.error(f"HTTP Error: {err}. Status code: {status_code}.")
        else:
            _log.error(f"HTTP Error with no response: {err}")
    elif isinstance(err, requests.exceptions.ConnectionError):
        _log.error(f"Connection Error: {err}. Unable to reach the server.")
    elif isinstance(err, requests.exceptions.Timeout):
        _log.error(f"Timeout Error: {err}. The request timed out.")
    else:
        _log.error(f"Request Exception: {err}. An unknown error occurred.")


# Used by functions that accept parameters used in GUI Simple Search
# e.g. getSimpleSearch()
class Criteria(Enum):
    Collection = "CollectionCriteria"
    Species = "SpeciesCriteria"
    ImageModality = "ImageModalityCriteria"
    BodyPart = "AnatomicalSiteCriteria"
    Manufacturer = "ManufacturerCriteria"
    DateRange = "DateRangeCriteria"
    Patient = "PatientCriteria"
    NumStudies = "MinNumberOfStudiesCriteria"
    ModalityAnded = "ModalityAndedSearchCriteria"

# Used by getSimpleSearch() to transform
#   species codes into human readable terms
NPEXSpecies = {
    "human": 337915000
    , "mouse": 447612001
    , "dog": 448771007
}


def setApiUrl(endpoint, api_url = "services"):
    """
    setApiUrl() is used by most other functions to select the correct base URL
    and is generally not something that needs to be called directly in your code.

    It assists with:
        1. selecting the correct base URL for regular collections vs NLST
        2. token refreshes (no longer necessary for regular users)

    Learn more about the NBIA APIs at https://wiki.cancerimagingarchive.net/x/ZoATBg
    """
    # Validate api_url
    # Convert empty string to default to services
    if not api_url:
        api_url = "services"
        
    valid_api_urls = ["services", "nlst"]
    if api_url not in valid_api_urls:
        raise ValueError(
            f"Invalid api_url '{api_url}'. Must be one of: {valid_api_urls}"
        )
    
    hostname = f"https://{api_url}.cancerimagingarchive.net/nbia-api/services"
    
    # Some endpoints were not migrated to v4 API (and may never be)
    if endpoint == "getContentsByName":
        base_url = f"{hostname}/v1/"
    else:
        base_url = f"{hostname}/v4/"
        
    return base_url


def queryData(
    endpoint: str,
    options: dict,
    api_url: str,
    format: str = "json",
    method: str = "GET",
    param: Optional[dict] = None,
    max_rows: int = 20
) -> Optional[Union[dict, pd.DataFrame, HTML]]:
    """
    Sends an HTTP request to the specified endpoint and formats the response.

    Args:
        endpoint (str): The API endpoint to query.
        options (dict): Query parameters for GET requests.
        api_url (str): Base URL of the API.
        format (str): Format of the output. Options are "json", "df" (DataFrame), 
                      "csv", or "html". Defaults to "json".
        method (str): HTTP method to use. Options are "GET" or "POST". Defaults to "GET".
        param (Optional[dict]): Form data for POST requests. Defaults to None.
        max_rows (int): The maximum number of rows to display for the 'html' format.
                         Defaults to 20.

    Returns:
        Optional[Union[dict, pd.DataFrame, HTML]]: The API response in the requested format, 
        or None if an error occurs.
    """
    base_url = setApiUrl(endpoint, api_url)
    url = f"{base_url}{endpoint}"
    response = None

    try:
        if method.upper() == "POST":
            _log.info(f'Calling {endpoint} with parameters {param}')
            response = requests.post(url, data=param)
        else:
            _log.info(f'Calling {endpoint} with parameters {options}')
            response = requests.get(url, params=options)

        response.raise_for_status()

        if not response.content.strip():
            _log.info("No results found.")
            return None

        try:
            data = response.json()
        except ValueError:
            _log.error(f"Failed to decode JSON from response. Response text: {response.text}")
            return None

        # --- Formatting Logic ---
        if format.lower() in ["df", "csv", "html"]:
            df = pd.DataFrame(data)
            if 'Series UID' in df.columns:
                df.rename(columns={'Series UID': 'SeriesInstanceUID'}, inplace=True)

            if format.lower() == "csv":
                csv_filename = f"{endpoint}_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv"
                df.to_csv(csv_filename, index=False)
                _log.info(f"CSV saved to: {csv_filename}")
                return df

            if format.lower() == "html":              
                # 1. Determine the display DataFrame and slice it *before* processing.
                if len(df) > max_rows:
                    _log.warning(f"Previewing the first {max_rows} of {len(df)} results. Use your query in combination with idcOhifViewer() for more flexibility.")
                    # Use .copy() to avoid a potential SettingWithCopyWarning
                    df_display = df.head(max_rows).copy()
                else:
                    df_display = df.copy()

                # 2. Now, run the expensive link-creation step *only on the small, sliced DataFrame*.
                base_url = "https://viewer.imaging.datacommons.cancer.gov/v3/viewer/"
                
                if 'SeriesInstanceUID' in df_display.columns and 'StudyInstanceUID' in df_display.columns:
                    df_display['SeriesInstanceUID'] = df_display.apply(
                        lambda row: f'<a href="{base_url}?StudyInstanceUIDs={row["StudyInstanceUID"]}&SeriesInstanceUIDs={row["SeriesInstanceUID"]}" target="_blank">{row["SeriesInstanceUID"]}</a>',
                        axis=1
                    )
                
                if 'StudyInstanceUID' in df_display.columns:
                    df_display['StudyInstanceUID'] = df_display['StudyInstanceUID'].apply(
                        lambda uid: f'<a href="{base_url}?StudyInstanceUIDs={uid}" target="_blank">{uid}</a>'
                    )

                # 3. Convert the processed small DataFrame to HTML.
                html_output = df_display.to_html(escape=False, index=False)
                return HTML(html_output)

            # This handles format == "df"
            return df
        else:
            # This handles format == "json"
            return data

    except requests.exceptions.RequestException as err:
        log_request_exception(err)
        return None


def getCollections(api_url = "",
                   format = ""):
    """
    Gets a list of collections from a specified api_url
    """
    endpoint = "getCollectionValues"
    options = {}

    data = queryData(endpoint, options, api_url, format)
    return data


def getBodyPart(collection = "",
                modality = "",
                api_url = "",
                format = ""):
    """
    Gets Body Part Examined metadata from a specified api_url.
    Allows filtering by collection and modality.
    """
    endpoint = "getBodyPartValues"

    # create options dict to construct URL
    options = {}

    if collection:
        options['Collection'] = collection
    if modality:
        options['Modality'] = modality

    data = queryData(endpoint, options, api_url, format)
    return data


def getModality(collection = "",
                bodyPart = "",
                api_url = "",
                format = ""):
    """
    Gets Modalities metadata from a specified api_url.
    Allows filtering by collection and bodyPart.
    """
    endpoint = "getModalityValues"

    # create options dict to construct URL
    options = {}

    if collection:
        options['Collection'] = collection
    if bodyPart:
        options['BodyPartExamined'] = bodyPart

    data = queryData(endpoint, options, api_url, format)
    return data


def getPatient(collection = "",
               api_url = "",
               format = ""):
    """
    Gets Patient metadata from a specified api_url.
    Allows filtering by collection.
    """
    endpoint = "getPatient"

    # create options dict to construct URL
    options = {}

    if collection:
        options['Collection'] = collection

    data = queryData(endpoint, options, api_url, format)
    return data


def getPatientByCollectionAndModality(collection,
                                      modality,
                                      api_url = "",
                                      format = ""):
    """
    Requires specifying collection and modality.
    Gets Patient IDs from a specified api_url.
    Returns a list of patient IDs.
    """
    endpoint = "getPatientByCollectionAndModality"

    # create options dict to construct URL
    options = {}
    options['Collection'] = collection
    options['Modality'] = modality

    data = queryData(endpoint, options, api_url, format)
    return data


def getNewPatientsInCollection(collection,
                               date,
                               api_url = "",
                               format = ""):
    """
    Gets "new" patient metadata from a specified api_url.
    Requires specifying a collection and release date.
    The date format is YYYY/MM/DD.
    """
    endpoint = "NewPatientsInCollection"
    
    # Convert user-provided YYYY/MM/DD to the API's required MM/DD/YYYY format.
    nbiaDate = datetime.strptime(date, "%Y/%m/%d").strftime("%m-%d-%Y")
    
    # create options dict to construct URL
    options = {}
    options['Collection'] = collection
    options['fromDate'] = nbiaDate

    data = queryData(endpoint, options, api_url, format)
    return data


def getStudy(collection,
             patientId = "",
             studyUid = "",
             api_url = "",
             format = ""):
    """
    Gets Study (visit/timepoint) metadata from a specified api_url
    Requires a collection parameter.
    Optional: patientId, studyUid, api_url, format
    """
    endpoint = "getPatientStudy"

    # create options dict to construct URL
    options = {}
    options['Collection'] = collection

    if patientId:
        options['PatientID'] = patientId
    if studyUid:
        options['StudyInstanceUID'] = studyUid

    data = queryData(endpoint, options, api_url, format)
    return data


def getNewStudiesInPatient(collection,
                           patientId,
                           date,
                           api_url = "",
                           format = ""):
    """
    Gets "new" patient metadata from a specified api_url.
    Requires specifying collection, patient ID and date.
    The date format is YYYY/MM/DD.
    """
    endpoint = "NewStudiesInPatientCollection"

    # Convert user-provided YYYY/MM/DD to the API's required MM/DD/YYYY format.
    nbiaDate = datetime.strptime(date, "%Y/%m/%d").strftime("%m-%d-%Y")
    
    # create options dict to construct URL
    options = {}
    options['Collection'] = collection
    options['PatientID'] = patientId
    options['fromDate'] = nbiaDate

    data = queryData(endpoint, options, api_url, format)
    return data


def getSeries(collection = "",
              patientId = "",
              studyUid = "",
              seriesUid = "",
              modality = "",
              bodyPart = "",
              manufacturer = "",
              manufacturerModel = "",
              api_url = "",
              format = ""):
    """
    Gets Series (scan) metadata from a specified api_url.
    Allows filtering by collection, patient ID, study UID,
    series UID, modality, body part, manufacturer & model.
    """
    endpoint = "getSeries"

    # create options dict to construct URL
    options = {}

    if collection:
        options['Collection'] = collection
    if patientId:
        options['PatientID'] = patientId
    if studyUid:
        options['StudyInstanceUID'] = studyUid
    if seriesUid:
        options['SeriesInstanceUID'] = seriesUid
    if modality:
        options['Modality'] = modality
    if bodyPart:
        options['BodyPartExamined'] = bodyPart
    if manufacturer:
        options['Manufacturer'] = manufacturer
    if manufacturerModel:
        options['ManufacturerModelName'] = manufacturerModel

    data = queryData(endpoint, options, api_url, format)
    return data

def getUpdatedSeries(date,
                     api_url = "",
                     format = ""):
    """
    Gets "new" series metadata from a specified api_url.
    The date format is YYYY/MM/DD.
    NOTE: Unlike other API endpoints, this one expects DD/MM/YYYY,
      but we convert to YYYY/MM/DD so tcia-utils date inputs are consistent.
    """
    endpoint = "getUpdatedSeries"

    # Convert user-provided YYYY/MM/DD to the API's required MM/DD/YYYY format.
    nbiaDate = datetime.strptime(date, "%Y/%m/%d").strftime("%m-%d-%Y")

    # create options dict to construct URL
    options = {}
    options['fromDate'] = nbiaDate

    data = queryData(endpoint, options, api_url, format)
    return data

def getSeriesList(
    uids: List[str],
    api_url: str = "",
    csv_filename: str = ""
) -> Optional[pd.DataFrame]:
    """
    Retrieve metadata for a list of series from the API.

    Args:
        uids (List[str]): List of unique identifiers (series UIDs) to query.
        api_url (str, optional): Base URL of the API to be used for the requests. Defaults to an empty string.
        csv_filename (str, optional): Name of the CSV file to save results.

    Returns:
        Optional[pd.DataFrame]: A DataFrame containing the series metadata.
    """
    chunk_size = 10000
    chunked_uids = [uids[i:i + chunk_size] for i in range(0, len(uids), chunk_size)]

    if len(chunked_uids) > 1:
        _log.info(f"Your data has been split into {len(chunked_uids)} chunks for processing.")

    dfs = []
    endpoint = "getSeriesMetadata"

    # Process each chunk
    for idx, chunk in enumerate(chunked_uids, start=1):
        if len(chunked_uids) > 1:
            _log.info(f"Processing chunk {idx}/{len(chunked_uids)}...")

        uidList = ",".join(chunk)
        param = {'list': uidList}
        base_url = setApiUrl(endpoint, api_url)
        url = f"{base_url}{endpoint}"

        try:
            response = requests.post(url, data=param)
            response.raise_for_status()

            if response and not response.content.strip():
                _log.info(f"No results found for chunk {idx}.")
                continue

            df_chunk = pd.read_csv(io.StringIO(response.text), sep=',')
            dfs.append(df_chunk)

        except requests.exceptions.RequestException as err:
            log_request_exception(err)
            continue
        except Exception as e:
            _log.error(f"An error occurred while processing chunk {idx}: {e}")
            continue

    if not dfs:
        _log.error("No results returned for the provided series UIDs. Ensure you have access to the requested data.")
        return None

    df = pd.concat(dfs, ignore_index=True)

    # Standardize column names to match other functions
    column_mapping = {
        'Patient ID': 'PatientID',
        'Study Instance UID': 'StudyInstanceUID',
        'Series Instance UID': 'SeriesInstanceUID',
        'Study Date': 'StudyDate',
        'Series Date': 'SeriesDate',
        'Image Count': 'ImageCount',
        'File Size': 'FileSize',
        'Date Released': 'DateReleased',
        'Body Part Examined': 'BodyPartExamined',
        'Series Description': 'SeriesDescription',
        'Manufacturer Model Name': 'ManufacturerModelName',
        'Software Versions': 'SoftwareVersions',
        'License Name': 'LicenseName',
        'License URI': 'LicenseURI',
        'Collection URI': 'DataDescriptionURI',
    }
    df.rename(columns=column_mapping, inplace=True)


    if csv_filename:
        df.to_csv(f"{csv_filename}.csv", index=False)
        _log.info(f"Series metadata saved as '{csv_filename}.csv'")

    return df

def getSeriesSize(seriesUid,
                  api_url = "",
                  format = ""):
    """
    Optional: api_url, format
    Gets the file count and disk size of a series/scan

    Note: This API endpoint is extremely slow to run and size info is a return value
          in getSeries() so there isn't much reason to use this.
    """
    endpoint = "getSeriesSize"

    # create options dict to construct URL
    options = {}
    options['SeriesInstanceUID'] = seriesUid

    data = queryData(endpoint, options, api_url, format)
    return data


def getSopInstanceUids(seriesUid,
                       api_url = "",
                       format = ""):
    """
    Optional: api_url, format
    Gets SOP Instance UIDs from a specific series/scan
    """
    endpoint = "getSOPInstanceUIDs"

    # create options dict to construct URL
    options = {}
    options['SeriesInstanceUID'] = seriesUid

    data = queryData(endpoint, options, api_url, format)
    return data


def getManufacturer(collection = "",
                    modality = "",
                    bodyPart = "",
                    api_url = "",
                    format = ""):
    """
    All parameters are optional.
    Gets manufacturer metadata from a specified api_url.
    Allows filtering by collection, body part & modality.
    """
    endpoint = "getManufacturerValues"

    # create options dict to construct URL
    options = {}

    if collection:
        options['Collection'] = collection
    if modality:
        options['Modality'] = modality
    if bodyPart:
        options['BodyPartExamined'] = bodyPart

    data = queryData(endpoint, options, api_url, format)
    return data


def getSharedCart(name,
                  api_url = "",
                  format = ""):
    """
    Optional: api_url, format
    Gets "Shared Cart" metadata from a specified api_url.
    First use https://nbia.cancerimagingarchive.net/nbia-search/ in a browser,
    then add data to your cart and click "Share" > "Share my cart".
    This creates a shared cart with a URL like,
    https://nbia.cancerimagingarchive.net/nbia-search/?saved-cart=nbia-49121659384603347
    You can then use this with a getSharedCart "name" of "nbia-49121659384603347".
    """
    endpoint = "getContentsByName"

    # create options dict to construct URL
    options = {}
    options['name'] = name

    data = queryData(endpoint, options, api_url, format)
    return data


def _download_and_unzip_series(seriesUID, path, api_url, downloadOptions, as_zip):
    """Helper function to download and optionally unzip a single series."""
    try:
        endpoint = "getImage"
        pathTmp = os.path.join(path, seriesUID)
        zip_path = f"{pathTmp}.zip"
        base_url = setApiUrl(endpoint, api_url)
        data_url = base_url + downloadOptions + seriesUID

        _log.info(f"Downloading... {data_url}")
        data = requests.get(data_url)

        if data.status_code == 200:
            if as_zip:
                with open(zip_path, "wb") as zip_file:
                    zip_file.write(data.content)
            else:
                with zipfile.ZipFile(io.BytesIO(data.content)) as file:
                    file.extractall(path=pathTmp)
            return seriesUID  # Return UID on success
        else:
            _log.error(f"Error: {data.status_code} Series failed: {seriesUID}")
            return None  # Return None on failure
    except Exception as e:
        _log.error(f"Exception during download for series {seriesUID}: {e}")
        return None


def downloadSeries(series_data: Union[str, pd.DataFrame, List[str]],
                   number: int = 0,
                   path: str = "tciaDownload",
                   hash: str = "",
                   api_url: str = "",
                   input_type: str = "",
                   format: str = "",
                   csv_filename: str = "",
                   as_zip: bool = False,
                   max_workers: int = 10) -> Union[pd.DataFrame, None]:
    """
    Ingests a set of seriesUids and downloads them.
    By default, series_data expects JSON containing "SeriesInstanceUID" elements.
    Set number = n to download the first n series if you don't want the full dataset.
    Set hash = "y" if you'd like to retrieve MD5 hash values for each image.
    Saves to tciaDownload folder in current directory if no path is specified.
    Set input_type = "list" to pass a list of Series UIDs instead of JSON.
    Set input_type = "df" to pass a dataframe that contains a "SeriesInstanceUID" column.
    Set input_type = "manifest" to pass the path of a *.TCIA manifest file as series_data.
    Format can be set to "df" or "csv" to return series metadata. The metadata
      includes info about series that have previously been downloaded if they're part of series_data.
    Setting a csv_filename will create the csv even if format isn't specified.
    If `as_zip` is set to True, it skips the unzipping steps.
    """
    # Prep a dataframe for later
    manifestDF = pd.DataFrame() if format in ["df", "csv"] or csv_filename else None

    # Convert the input data to a python list of uids
    try:
        if input_type == "manifest":
            series_data = manifestToList(series_data)
        elif input_type == "df":
            series_data = series_data['SeriesInstanceUID'].tolist()
        elif input_type == "list":
            pass  # series_data is already a list
        else:
            series_data = [item['SeriesInstanceUID'] for item in series_data]

    except ValueError as e:
        _log.error(f"Error parsing series_data: {e}")
        return None

    # Ensure the root directory exists
    try:
        if not os.path.exists(path):
            os.makedirs(path)
            _log.info(f"Directory '{path}' created successfully.")
        else:
            _log.info(f"Directory '{path}' already exists.")
    except OSError as e:
        _log.error(f"Failed to create directory '{path}': {e}")
        return None

    # Identify series to download vs. already existing
    existing_files = set(os.listdir(path))
    uids_to_download = []
    previously_downloaded_uids = []
    for seriesUID in series_data:
        unzipped_exists = seriesUID in existing_files
        zip_exists = f"{seriesUID}.zip" in existing_files
        if not unzipped_exists and not zip_exists:
            uids_to_download.append(seriesUID)
        else:
            if unzipped_exists:
                _log.warning(f"Series {seriesUID} already downloaded and unzipped.")
            elif zip_exists:
                _log.warning(f"Series {seriesUID} already downloaded as a zip file.")
            if manifestDF is not None:
                previously_downloaded_uids.append(seriesUID)

    # Apply 'number' limit if specified
    if number > 0:
        uids_to_download = uids_to_download[:number]

    _log.info(f"Found {len(previously_downloaded_uids)} previously downloaded series.")
    _log.info(f"Attempting to download {len(uids_to_download)} new series.")

    # Concurrently download new series
    successfully_downloaded_uids = []
    failed_downloads = 0
    if uids_to_download:
        downloadOptions = "getImageWithMD5Hash?SeriesInstanceUID=" if hash == "y" else "getImage?NewFileNames=Yes&SeriesInstanceUID="
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_uid = {executor.submit(_download_and_unzip_series, uid, path, api_url, downloadOptions, as_zip): uid for uid in uids_to_download}

            for future in concurrent.futures.as_completed(future_to_uid):
                result_uid = future.result()
                if result_uid:
                    successfully_downloaded_uids.append(result_uid)
                else:
                    failed_downloads += 1

    # Fetch metadata for all series if needed
    if manifestDF is not None:
        # Batch fetch metadata for existing and newly downloaded series
        uids_for_metadata = previously_downloaded_uids + successfully_downloaded_uids
        if uids_for_metadata:
            df_meta = getSeriesList(uids=uids_for_metadata, api_url=api_url)
            if df_meta is not None:
                manifestDF = pd.concat([manifestDF, df_meta], ignore_index=True)

    # Summarize and return
    success = len(successfully_downloaded_uids)
    previous = len(previously_downloaded_uids)
    failed = failed_downloads
    _log.info(
        f"Downloaded {success} out of {len(uids_to_download)} targeted series.\n"
        f"{failed} failed to download.\n"
        f"{previous} were previously downloaded."
    )

    # Return metadata dataframe and/or save to CSV file if requested
    if manifestDF is not None:
        if csv_filename:
            manifestDF.to_csv(csv_filename + '.csv')
            _log.info(f"Series metadata saved as {csv_filename}.csv")
        elif format == "csv":
            dt_string = datetime.now().strftime("%Y-%m-%d_%H%M")
            manifestDF.to_csv(f'downloadSeries_metadata_{dt_string}.csv')
            _log.info(f"Series metadata saved as downloadSeries_metadata_{dt_string}.csv")
        return manifestDF if format == "df" or format == "csv" else None


def downloadImage(seriesUID: str, sopUID: str, path: Optional[str] = "", api_url: Optional[str] = "") -> None:
    """
    Downloads a DICOM image from a specified API using the provided SeriesInstanceUID and SOPInstanceUID.

    Args:
        seriesUid (str): The SeriesInstanceUID of the DICOM series.
        sopUid (str): The SOPInstanceUID of the DICOM image.
        path (Optional[str]): The directory path where the image will be saved. Defaults to "tciaDownload".
        api_url (Optional[str]): The base URL of the API. If not provided, a default URL will be used.

    Raises:
        requests.exceptions.RequestException: If there is an error during the HTTP request.

    Example:
        downloadImage("1.2.840.113619.2.55.3.604688.1234.5678.91011", "1.2.840.113619.2.55.3.604688.1234.5678.91012")
    """
    endpoint = "getSingleImage"

    # get base URL
    base_url = setApiUrl(endpoint, api_url)

    try:
        path_tmp = os.path.join(path or "tciaDownload", seriesUID)
        file = f"{sopUID}.dcm"
        file_path = os.path.join(path_tmp, file)

        if not os.path.isfile(file_path):
            data_url = f"{base_url}getSingleImage?SeriesInstanceUID={seriesUID}&SOPInstanceUID={sopUID}"
            _log.info(f"Downloading... {data_url}")
            data = requests.get(data_url)

            if data.status_code == 200:
                os.makedirs(path_tmp, exist_ok=True)
                with open(file_path, 'wb') as f:
                    f.write(data.content)
                _log.info(f"Saved to {file_path}")
            else:
                _log.error(
                    f"Error: {data.status_code} -- double check your permissions and Series/SOP UIDs.\n"
                    f"Series UID: {seriesUID}\n"
                    f"SOP UID: {sopUID}"
                )
        else:
            _log.warning(f"Image {sopUID} already downloaded to:\n{path_tmp}")

    except requests.exceptions.RequestException as err:
        return log_request_exception(err)


##########################
##########################
# Advanced API Endpoints


def getCollectionDescriptions(api_url = "", format = "", removeHtml = None):
    """
    Gets HTML-formatted descriptions of collections and their DOIs
    """
    endpoint = "getCollectionDescriptions"
    options = {}

    data = queryData(endpoint, options, api_url, format)
    if format == "df" and removeHtml == "yes":
        data['description'] = data['description'].apply(remove_html_tags)
        return data
    else:
        return data


def getCollectionPatientCounts(api_url = "", format = ""):
    """
    Gets counts of Patient by collection from Advanced API
    """
    endpoint = "getCollectionValuesAndCounts"
    options = {}

    data = queryData(endpoint, options, api_url, format)
    return data


def getModalityCounts(collection = "",
                      bodyPart = "",
                      api_url = "",
                      format = ""):
    """
    Gets counts of Modality metadata from Advanced API
    Allows filtering by collection and bodyPart
    """
    endpoint = "getModalityValuesAndCounts"

    # create options dict to construct URL
    options = {}

    if collection:
        options['Collection'] = collection
    if bodyPart:
        options['BodyPartExamined'] = bodyPart

    data = queryData(endpoint, options, api_url, format)
    return data


def getBodyPartCounts(collection = "",
                      modality = "",
                      api_url = "",
                      format = ""):
    """
    Gets counts of Body Part metadata from Advanced API.
    Allows filtering by collection and modality.
    """
    endpoint = "getBodyPartValuesAndCounts"

    # create options dict to construct URL
    options = {}

    if collection:
        options['Collection'] = collection
    if modality:
        options['Modality'] = modality

    data = queryData(endpoint, options, api_url, format)
    return data


def getManufacturerCounts(collection = "",
                      modality = "",
                      bodyPart = "",
                      api_url = "",
                      format = ""):
    """
    Gets counts of Manufacturer metadata from Advanced API.
    Allows filtering by collection, body part and modality.
    """
    endpoint = "getManufacturerValuesAndCounts"

    # create options dict to construct URL
    options = {}

    if collection:
        options['Collection'] = collection
    if modality:
        options['Modality'] = modality
    if bodyPart:
        options['BodyPartExamined'] = bodyPart

    data = queryData(endpoint, options, api_url, format)
    return data


def getDicomTags(seriesUid: str, api_url: str = "", format: str = "df") -> Optional[pd.DataFrame]:
    """
    Retrieves DICOM tag metadata for a given Series UID.

    Args:
        seriesUid (str): The UID of the DICOM series to query.
        api_url (str, optional): The base URL for the API. Defaults to "".
        format (str, optional): Desired format of the output. Defaults to "df" (DataFrame).

    Returns:
        Optional[pd.DataFrame]: A DataFrame containing DICOM tag metadata, or None if the query fails.
    """
    endpoint = "getDicomTags"
    options = {'SeriesUID': seriesUid}

    # Query the API
    data = queryData(endpoint, options, api_url, format)

    # Ensure a valid DataFrame is returned or log an error
    if isinstance(data, pd.DataFrame):
        return data
    elif data is None:
        _log.info(f"No data returned for Series UID: {seriesUid}")
        return None
    else:
        _log.error(f"Unexpected response format for Series UID: {seriesUid}")
        return None


def getSegRefSeries(uid):
    """
    Gets DICOM tag metadata for a given SEG/RTSTRUCT series UID (scan)
    and looks up the corresponding original/reference series UID

    Note: Since there are no SEG/RTSTRUCT series in the NLST server it is not queried.
    """
    # get dicom tags for the series as a dataframe
    df = getDicomTags(uid, format="df")

    if df is not None:
        # Find the row where element = "(0008,0060) Modality"
        findModality = df['element'] == '(0008,0060)'
        modality = df.loc[findModality, 'data'].item()

        if modality == "RTSTRUCT":
            # Locate "RT Referenced Series Sequence >>(3006,0014)"
            # and "Series Instance UID >>>(0020,000E)" in the dataframe
            refElements = (df['element'] == '>>(3006,0014)') & (df['element'].shift(-1) == '>>>(0020,000E)')

            if refElements.any():
                # Get the index for end of segmentation sequence
                index = df.index[refElements].item()

                # Retrieve the value of "Series Instance UID" from the next row
                refSeriesUid = df.loc[index + 1, 'data']
                return refSeriesUid

            else:
                _log.warning(f"Series {uid} does not contain a Reference Series UID.")
                refSeriesUid = "N/A"
                return refSeriesUid

        elif modality == "SEG":
            # Locate ">(0008,114A) End Referenced Instance Sequence"
            # and ">(0020,000E) Series Instance UID" in the dataframe
            refElements = (df['element'] == '>(0008,114A)') & (df['element'].shift(-1) == '>(0020,000E)')

            if refElements.any():
                # Get the index for end of segmentation sequence
                index = df.index[refElements].item()

                # Retrieve the value of "Series Instance UID" from the next row
                refSeriesUid = df.loc[index + 1, 'data']
                return refSeriesUid

            else:
                _log.warning(f"Series {uid} does not contain a Reference Series UID.")
                refSeriesUid = "N/A"
                return refSeriesUid

        else:
            _log.warning(f"Series {uid} is not a SEG/RTSTRUCT segmentation.")
            refSeriesUid = "N/A"
            return refSeriesUid

    else:
        _log.warning(f"Series {uid} couldn't be found.")
        refSeriesUid = "N/A"
        return refSeriesUid


def getSimpleSearch(
    collections = [],
    species = [],
    modalities = [],
    bodyParts = [],
    manufacturers  = [],
    fromDate = "",
    toDate = "",
    patients = [],
    minStudies: int = 0,
    modalityAnded = False,
    start = 0,
    size = 10,
    sortDirection = 'ascending',
    sortField = 'subject',
    api_url = "",
    format = ""):
    """
    All parameters are optional.
    Takes the same parameters as the SimpleSearch GUI
    Use more parameters to narrow the number of subjects received.

    collections: list[str]   -- The DICOM collections of interest to you
    species: list[str]       -- Filter collections by species. Possible values are 'human', 'mouse', and 'dog'
    modalities: list[str]    -- Filter collections by modality
    modalityAnded: bool      -- If true, only return subjects with all requested modalities, as opposed to any
    minStudies: int          -- The minimum number of studies a collection must have to be included in the results
    manufacturers: list[str] -- Imaging device manufacturers, e.g. SIEMENS
    bodyParts: list[str]     -- Body parts of interest, e.g. CHEST, ABDOMEN
    fromDate: str            -- First cutoff date, in YYYY/MM/DD format. Defaults to 1900/01/01
    toDate: str              -- Second cutoff date, in YYYY/MM/DD format. Defaults to today's date
    patients: list[str]      -- Patients to include in the output
    start: int               -- Start of returned series page. Defaults to 0.
    size: int                -- Size of returned series page. Defaults to 10.
    sortDirection            -- 'ascending' or 'descending'. Defaults to 'ascending'.
    sortField                -- 'subject', 'studies', 'series', or 'collection'. Defaults to 'subject'.
    format: str              -- Defaults to JSON. Can be set to "uids" to return a python list of
                                Series Instance UIDs or "manifest" to save a TCIA manifest file (up to 1,000,000 series).
                                "manifest_text" can be used to return the manifest content as text rather than saving it to disk.

    Example call: getSimpleSearch(collections=["TCGA-UCEC", "4D-Lung"], modalities=["CT"])
    """
    if format in ["manifest", "manifest_text", "uids"]:
        endpoint = "getManifestForSimpleSearch"
        size = 1000000
    else:
        endpoint = "getSimpleSearch"

    criteriaTypeIndex = 0
    options = {}

    getCriteria = lambda: "".join(['criteriaType', str(criteriaTypeIndex)])
    getValue = lambda: "".join(['value', str(criteriaTypeIndex)])
    getCriteriaAndValue = lambda: (getCriteria(), getValue())
    def setOptionValue(criteriaType, param):
        criteria, value = getCriteriaAndValue()
        options[criteria] = str(criteriaType.value)
        options[value] = param

    if fromDate or toDate:
        from_date, to_date, bad_dates = None, None, []
        if toDate and not fromDate:
            _log.info("No fromDate specified, using 1900/01/01")
            fromDate = "1900/01/01"
        if fromDate and not toDate:
            _log.info("No toDate specified, using today's date")
            toDate = datetime.now().strftime("%Y/%m/%d")
        try:
            from_date = datetime.strptime(fromDate, "%Y/%m/%d")
        except ValueError:
            bad_dates.append("fromDate")
        try:
            to_date = datetime.strptime(toDate, "%Y/%m/%d")
        except ValueError:
            bad_dates.append("toDate")
        if bad_dates:
            _log.error(f'Malformed date parameter(s) {bad_dates}; use Y/m/d format e.g. 1999/12/31')
            raise StopExecution

        # NBIA switched to MM-DD-YYYY in v4 API, so we'll convert to retain tcia_utils format
        nbiaFromDate = datetime.strptime(fromDate, "%Y/%m/%d").strftime("%m-%d-%Y")
        nbiaToDate = datetime.strptime(toDate, "%Y/%m/%d").strftime("%m-%d-%Y")

        criteria = getCriteria()
        options[criteria] = Criteria.DateRange
        options["fromDate" + str(criteriaTypeIndex)] = nbiaFromDate
        options["toDate" + str(criteriaTypeIndex)] = nbiaToDate
        criteriaTypeIndex += 1
    
    if collections:
        for collection in collections:
            setOptionValue(Criteria.Collection, collection)
            criteriaTypeIndex += 1
    if species:
        for val in species:
            setOptionValue(Criteria.Species, NPEXSpecies[val])
            criteriaTypeIndex += 1
    if modalities:
        for modality in modalities:
            setOptionValue(Criteria.ImageModality, modality)
            criteriaTypeIndex += 1
    if bodyParts:
        for bodyPart in bodyParts:
            setOptionValue(Criteria.BodyPart, bodyPart)
            criteriaTypeIndex += 1
    if manufacturers:
        for manufacturer in manufacturers:
            setOptionValue(Criteria.Manufacturer, manufacturer)
            criteriaTypeIndex += 1
    if patients:
        for patient in patients:
            setOptionValue(Criteria.Patient, patient)
            criteriaTypeIndex += 1
    if minStudies:
        setOptionValue(Criteria.NumStudies, minStudies)
        criteriaTypeIndex += 1
    if modalityAnded:
        setOptionValue(Criteria.ModalityAnded, "all")
        criteriaTypeIndex += 1
 
    options['sortField'] = sortField
    options['sortDirection'] = sortDirection
    options['tool'] = "tcia_utils"
    options['start'] = start
    options['size'] = size

    # set base_url
    base_url = setApiUrl(endpoint, api_url)

    # full url
    url = base_url + endpoint
    _log.info(f'Calling... {url}')

    # get data & handle any request.post() errors
    try:
        metadata = requests.post(url, data = options)
        metadata.raise_for_status()

        # check for empty results and format output
        if metadata.text and metadata.text != "[]":
            # format the output (optional)
            if format == "manifest":
                # Get the current date and time
                now = datetime.now()
                # Format it as 'manifest-YYYY-MM-DD_HH-MM.tcia'
                filename = now.strftime("manifest-%Y-%m-%d_%H-%M.tcia")
                # Save the manifest file
                with open(filename, 'w') as file:
                    file.write(metadata.text)
                _log.info(f"Manifest saved as {filename}")
            elif format == "manifest_text":
                return metadata.text
            elif format == "uids":
                # Discard the first 6 lines of config in manifest
                lines = metadata.text.split('\n')[6:]
                # Convert the Series UIDs to a list
                uids = [line for line in lines if line.strip()]
                return uids
            else:
                metadata = metadata.json()
                return metadata
        else:
            _log.info("No results found.")

    except requests.exceptions.RequestException as err:
        return log_request_exception(err)


##########################
##########################
# Reports


def formatSeriesInput(series_data, input_type, api_url):
    """
    Helper function to convert the various types of series metadata
    inputs and to standardize the data elements that come from those
    inputs into a uniform dataframe output that is harmonized to
    the fields from getSeries() in order to be ingested by other functions.
    Missing fields are set to None.
    series_data can be provided as JSON (default), df, TCIA manifest file or python list.
    input_type informs the function which of those types are being used.
    If input_type = "manifest" the series_data should be the path to the manifest file.
    """
    # if input_type is manifest convert it to a list
    if input_type == "manifest":
        series_data = manifestToList(series_data)
    # download relevant metadata for lists and manifests converted to lists
    if input_type == "list" or input_type == "manifest":
        df = getSeriesList(series_data, api_url=api_url)
    # pass the dataframe through if one was provided
    elif input_type == "df":
        df = series_data
    # create a dataframe if json was provided
    else:
        # Create a DataFrame from the series_data
        df = pd.DataFrame(series_data)

    # Check if data is empty
    if df is None or df.empty:
        _log.error(f"No data was provided for reformatting.")
        raise StopExecution

    # Updated column mapping to handle all three API endpoint formats
    column_mapping = {
        # Core identifiers - getSeriesMetadata() format -> standard format
        'Patient ID': 'PatientID',
        'Subject ID': 'PatientID',  # Alternative naming
        'Series Instance UID': 'SeriesInstanceUID',
        'Series ID': 'SeriesInstanceUID',  # Alternative naming
        'Study Instance UID': 'StudyInstanceUID',
        'Study UID': 'StudyInstanceUID',  # Alternative naming
        'Collection Name': 'Collection',

        # Study information
        'Study Description': 'StudyDesc',
        'Study Date': 'StudyDate',

        # Series information
        'Series Description': 'SeriesDescription',
        'Series Date': 'SeriesDate',
        'Series Number': 'SeriesNumber',
        'Protocol Name': 'ProtocolName',
        'Body Part Examined': 'BodyPartExamined',

        # Technical metadata
        'Manufacturer Model Name': 'ManufacturerModelName',
        'Software Versions': 'SoftwareVersions',

        # File information
        'Number of images': 'ImageCount',
        'Image Count': 'ImageCount',  # Alternative naming
        'File Size (Bytes)': 'FileSize',
        'File Size': 'FileSize',  # Alternative naming without units

        # Licensing and release info
        'License Name': 'LicenseName',
        'License URL': 'LicenseURI',
        'License URI': 'LicenseURI',  # Alternative naming
        'Data Description URI': 'DataDescriptionURI',
        'Date Released': 'DateReleased',
        'Released Status': 'ReleasedStatus',
        '3rd Party Analysis': 'ThirdPartyAnalysis',
        'Third Party Analysis': 'ThirdPartyAnalysis',
        'Annotations Flag': 'AnnotationsFlag',

        # Patient demographics (from getSeriesMetadata)
        'Patient Name': 'PatientName',
        'Patient Sex': 'PatientSex',
        'Patient Age': 'PatientAge',
        'Ethnic Group': 'EthnicGroup',
        'Species Code': 'SpeciesCode',
        'Species Description': 'SpeciesDescription',

        # Study details (from getSeriesMetadata)
        'Study ID': 'StudyID',
        'Admitting Diagnosis Description': 'AdmittingDiagnosisDescription',
        'Longitudinal Temporal Event Type': 'LongitudinalTemporalEventType',
        'Longitudinal Temporal Offset From Event': 'LongitudinalTemporalOffsetFromEvent',

        # Technical imaging parameters (from getSeriesMetadata)
        'Pixel Spacing(mm)- Row': 'PixelSpacingRow',
        'Slice Thickness(mm)': 'SliceThickness',
        'Max Submission Timestamp': 'MaxSubmissionTimestamp',

        # Site information
        'Site': 'Site',
        'Phantom': 'Phantom'
    }

    # Rename the columns in the DataFrame
    df.rename(columns=column_mapping, inplace=True)

    # Define all possible columns that should exist after normalization
    # Core columns (always expected)
    core_columns = [
        'SeriesInstanceUID', 'StudyInstanceUID', 'PatientID', 'Collection',
        'Modality', 'Manufacturer', 'TimeStamp', 'DataDescriptionURI',
        'LicenseName', 'LicenseURI', 'FileSize', 'ImageCount', 'DateReleased',
        'BodyPartExamined', 'SeriesDescription', 'StudyDesc', 'StudyDate', 'Authorized'
    ]

    # Extended columns (may be present depending on API endpoint)
    extended_columns = [
        'SeriesDate', 'SeriesNumber', 'ProtocolName', 'ManufacturerModelName',
        'SoftwareVersions', 'ThirdPartyAnalysis', 'AnnotationsFlag', 'ReleasedStatus',
        'PatientName', 'PatientSex', 'PatientAge', 'EthnicGroup', 'SpeciesCode',
        'SpeciesDescription', 'StudyID', 'AdmittingDiagnosisDescription',
        'LongitudinalTemporalEventType', 'LongitudinalTemporalOffsetFromEvent',
        'PixelSpacingRow', 'SliceThickness', 'MaxSubmissionTimestamp', 'Site', 'Phantom'
    ]

    all_possible_columns = core_columns + extended_columns

    # Add missing columns and set them to None
    for col in all_possible_columns:
        if col not in df.columns:
            df[col] = None

    # Data cleaning and formatting
    # Make all URLs lower case (handle NaN values)
    if 'DataDescriptionURI' in df.columns:
        df['DataDescriptionURI'] = df['DataDescriptionURI'].astype(str).str.lower()
        df['DataDescriptionURI'] = df['DataDescriptionURI'].replace('nan', None)

    if 'LicenseURI' in df.columns:
        df['LicenseURI'] = df['LicenseURI'].astype(str).str.lower()
        df['LicenseURI'] = df['LicenseURI'].replace('nan', None)

    # Format date-related columns to datetime (handle various formats and NaN)
    date_columns = ['DateReleased', 'TimeStamp', 'StudyDate', 'SeriesDate', 'MaxSubmissionTimestamp']
    for date_col in date_columns:
        if date_col in df.columns and df[date_col].notna().any():
            try:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            except Exception as e:
                _log.warning(f"Could not convert {date_col} to datetime: {e}")

    # Convert numeric columns where appropriate
    numeric_columns = ['FileSize', 'ImageCount', 'SeriesNumber', 'PatientAge', 'Authorized']
    for num_col in numeric_columns:
        if num_col in df.columns and df[num_col].notna().any():
            try:
                df[num_col] = pd.to_numeric(df[num_col], errors='coerce')
            except Exception as e:
                _log.warning(f"Could not convert {num_col} to numeric: {e}")

    # Clean up string columns - strip whitespace
    string_columns = [col for col in df.columns if col not in date_columns + numeric_columns]
    for str_col in string_columns:
        if str_col in df.columns and df[str_col].dtype == 'object':
            df[str_col] = df[str_col].astype(str).str.strip()
            df[str_col] = df[str_col].replace('nan', None)

    return df


def reportDoiSummary(series_data, input_type="", api_url = "", format=""):
    """
    Generate a summary report about DOIs from series metadata created by the
    output of getSeries(), getSeriesList(), a python list of Series UIDs, or
    from a TCIA manifest.

    Parameters:
    series_data: The input data to be summarized (expects JSON by default).
    input_type: Set to 'df' for dataframe.
                Set to 'list' for python list, or 'manifest' for *.TCIA manifest file.
                If manifest is used, series_data should be the path to the TCIA manifest file.
    format: Output format (default is dataframe, 'csv' for CSV file, 'chart' for charts).
    report_type: Defaults to summarizing by collection. Use 'doi' to group by DOIs.
                Helper functions reportCollectionSummary() and reportDoiSummary() are
                the expected way to deal with this, which pass this parameter accordingly.
    api_url: Only necessary if input_type = list or manifest.
            Set to 'restricted' for limited-access collections or
            'nlst' for National Lung Screening trial.

    See reportDataSummary() for more details.
    """
    df = reportDataSummary(series_data, input_type, report_type = "doi", api_url = api_url, format = format)
    return df


def reportCollectionSummary(series_data, input_type="", api_url = "", format=""):
    """
    Generate a summary report about Collections from series metadata created by the
    output of getSeries(), getSeriesList(), getSharedcart(), getUpdatedSeries(),
    a python list of Series UIDs, or from a TCIA manifest.

    Parameters:
    series_data: The input data to be summarized (expects JSON by default).
    input_type: Set to 'df' for dataframe.
                Set to 'list' for python list, or 'manifest' for *.TCIA manifest file.
                If manifest is used, series_data should be the path to the TCIA manifest file.
    format: Output format (default is dataframe, 'csv' for CSV file, 'chart' for charts).
    report_type: Defaults to summarizing by collection. Use 'doi' to group by DOIs.
                Helper functions reportCollectionSummary() and reportDoiSummary() are
                the expected way to deal with this, which pass this parameter accordingly.
    api_url: Only necessary if input_type = list or manifest.
            Set to 'restricted' for limited-access collections or
            'nlst' for National Lung Screening trial.

    See reportDataSummary() for more details.
    """

    df = reportDataSummary(series_data, input_type, report_type = "collection", api_url = api_url, format = format)
    return df


def reportDataSummary(series_data, input_type="", report_type = "", api_url = "", format=""):
    """
    This function summarizes the input series_data by reporting
    on the following attributes where available in series_data:
    - Collections: List of unique collections
    - DOIs: List of unique values
    - Modalities: List of unique values
    - Licenses: List of unique values
    - Manufacturers: List of unique values
    - Body Parts: List of unique values
    - Subjects: Number of subjects
    - Studies: Number of studies
    - Series: Number of series
    - Images: Number of images
    - Disk Space: Formatted as KB/MB/GB/TB/PB
    - DateReleased Min: Earliest data publication date
    - DateReleased Max: Latest publication date
    - UniqueDatesReleased: List of dates on which new series were published

    Parameters:
    series_data: The input data to be summarized (expects JSON by default).
    input_type: Set to 'df' for dataframe.
                Set to 'list' for python list, or 'manifest' for *.TCIA manifest file.
                If manifest is used, series_data should be the path to the TCIA manifest file.
    format: Output format (default is dataframe, 'csv' for CSV file, 'chart' for charts).
    report_type: Defaults to summarizing by collection. Use 'doi' to group by DOIs.
                Helper functions reportCollectionSummary() and reportDoiSummary() are
                the expected way to deal with this, which pass this parameter accordingly.
    api_url: Only necessary if input_type = list or manifest.
            Set to 'restricted' for limited-access collections or
            'nlst' for National Lung Screening trial.
    """
    # format series_data into df depending on input_type
    df = formatSeriesInput(series_data, input_type, api_url)

    # choose between collection report or DOI report
    # these are used later when renaming and formatting the columns/charts
    if report_type == "doi":
        group = "DataDescriptionURI"
        column = "Collection"
        columnGrouped = "Collections"
        chartLabel = "Identifier"
    else:
        group = "Collection"
        column = "DataDescriptionURI"
        columnGrouped = "DOIs"
        chartLabel = "Collection"

    # Group by Collection and calculate aggregated statistics
    grouped = df.groupby(group).agg({
        column: 'unique',
        'Modality': 'unique',
        'LicenseName': 'unique',
        'Manufacturer': 'unique',
        'BodyPartExamined': 'unique',
        'PatientID': 'nunique',
        'StudyInstanceUID': 'nunique',
        'SeriesInstanceUID': 'nunique',
        'ImageCount': 'sum',
        'FileSize': 'sum',
        'DateReleased': ['min', 'max']
    }).reset_index()

    # Flatten the multi-level DateReleased column and rename columns
    grouped.columns = [' '.join(col).strip() for col in grouped.columns.values]
    grouped.rename(columns={'DateReleased min': 'Min DateReleased',
                            'DateReleased max': 'Max DateReleased',
                            'PatientID nunique': 'Subjects',
                            'StudyInstanceUID nunique': 'Studies',
                            'SeriesInstanceUID nunique': 'Series',
                            'ImageCount sum': 'Images',
                            'FileSize sum': 'File Size'}
                            , inplace=True)

    # Create Disk Space column and convert bytes to MB/GB/TB/PB
    grouped['Disk Space'] = grouped['File Size'].apply(format_disk_space)

    try:
        # Extract unique submission dates per Collection
        df['DateReleased'] = pd.to_datetime(df['DateReleased'])
        unique_dates_df = df.groupby(group)['DateReleased'].apply(lambda x: x.dt.date.unique()).reset_index()

    except:
        # if DateReleased wasn't provided in series_data, condense None values to unique string
        unique_dates_df = df.groupby(group)['DateReleased'].apply(lambda x: x.unique()).reset_index()

    # rename columns
    unique_dates_df.columns = [group, 'UniqueDateReleased']

    # Merge the unique_dates_df with the grouped DataFrame
    grouped = grouped.merge(unique_dates_df, on=group, how='left')

    # Convert aggregated lists to strings & insert 'Not Specified' for null values
    grouped[columnGrouped] = grouped[column + ' unique'].apply(lambda x: ', '.join(['Not Specified' if pd.isnull(val) or val == '' else val for val in x]))
    grouped['Modalities'] = grouped['Modality unique'].apply(lambda x: ', '.join(['Not Specified' if pd.isnull(val) or val == '' else val for val in x]))
    grouped['Licenses'] = grouped['LicenseName unique'].apply(lambda x: ', '.join(['Not Specified' if pd.isnull(val) or val == '' else val for val in x]))
    grouped['Manufacturers'] = grouped['Manufacturer unique'].apply(lambda x: ', '.join(['Not Specified' if pd.isnull(val) or val == '' else val for val in x]))
    grouped['Body Parts'] = grouped['BodyPartExamined unique'].apply(lambda x: ', '.join(['Not Specified' if pd.isnull(val) or val == '' else val for val in x]))
    grouped['Min DateReleased'] = grouped['Min DateReleased'].apply(lambda x: 'Not Specified' if pd.isnull(x) or x == '' else x)
    grouped['Max DateReleased'] = grouped['Max DateReleased'].apply(lambda x: 'Not Specified' if pd.isnull(x) or x == '' else x)
    grouped['UniqueDateReleased'] = grouped['UniqueDateReleased'].apply(lambda x: ', '.join(sorted(['Not Specified' if pd.isnull(val) or val == '' else val.strftime('%Y-%m-%d') for val in x])) if len(x) > 1 else (x[0].strftime('%Y-%m-%d') if len(x) == 1 and not pd.isnull(x[0]) else 'Not Specified'))

    # Remove unnecessary columns
    grouped.drop(columns=[column + ' unique', 'Modality unique', 'LicenseName unique',
                        'Manufacturer unique', 'BodyPartExamined unique'], inplace=True)

    # Reorder the columns
    grouped = grouped[[group, columnGrouped, 'Licenses', 'Subjects', 'Studies', 'Series', 'Images', 'File Size', 'Disk Space',
            'Body Parts', 'Modalities',  'Manufacturers', 'Min DateReleased', 'Max DateReleased', 'UniqueDateReleased']]

    if report_type == "doi":
        # look up DOI info from datacite and create dataframe
        datacite = getDoi(format = "df")

        # drop unnecessary columns in datacite df
        datacite = datacite[["DOI", "Identifier"]]

        # Extract DOI from the end of DataDescriptionURI and store it in "DOI" column
        grouped["DOI"] = grouped["DataDescriptionURI"].str.extract(r'doi.org/(\S+)$')

        # format the DOI values consistently
        grouped['DOI'] = grouped['DOI'].str.strip()
        grouped['DOI'] = grouped['DOI'].str.lower()
        datacite['DOI'] = datacite['DOI'].str.strip()
        datacite['DOI'] = datacite['DOI'].str.lower()

        # Merge datacite with the df DataFrame
        grouped = grouped.merge(datacite, on='DOI', how='left')

        # drop DOI column
        grouped.drop(columns=['DOI'], inplace=True)

        # Move the 'Identifier' column to the first position
        cols = list(grouped.columns)
        cols.insert(0, cols.pop(cols.index('Identifier')))
        grouped = grouped[cols]

    # generate charts if requested
    if format == 'chart':

        # define datasets
        datasets = grouped[chartLabel].tolist()

        # Calculate the metrics
        subjects = grouped['Subjects'].tolist()
        studies = grouped['Studies'].tolist()
        series = grouped['Series'].tolist()
        images = grouped['Images'].tolist()
        size = grouped['File Size'].tolist()

        # Create separate pie charts for each metric
        data_label_pairs = [(subjects, 'Subjects'),
                            (studies, 'Studies'),
                            (series, 'Series'),
                            (images, 'Images'),
                            (size, 'File Size (Bytes)')]

        # Iterate through the pairs and call create_pie_chart if data is greater than 0
        for data, label in data_label_pairs:

            # Check if data is not empty and contains values greater than 0
            if data and all(x > 0 for x in data):
                create_pie_chart(data, label, datasets)
            else:
                _log.info("No data available for " + label)

    if format == 'csv':
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'tcia_{report_type}_report_{timestamp}.csv'
        grouped.to_csv(filename, index=False)
        _log.info(f"Collection summary report saved as '{filename}'")

    return grouped


def create_pie_chart(data, metric_name, labels, width=800, height=600):
    """
    Helper function for reportCollections() to create pie charts with plotly.
    """

    # Calculate the total sum of data points
    total = sum(data)

    # Create a DataFrame for the pie chart
    df = pd.DataFrame({'Labels': labels, 'Values': data})

    # Create the pie chart using Plotly
    fig = px.pie(df, names='Labels', values='Values', title=f'{metric_name} Distribution Across Datasets')
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(showlegend=True, legend_title_text='Datasets', width=width, height=height)

    # Show the pie chart
    fig.show()


def reportSeriesSubmissionDate(series_data, chart_width = 1024, chart_height = 768):
    """
    Ingests the results of getSeries() as df or JSON and visualizes the
    submission timeline of the series in it by collection.

    Currently this only supports getSeries(), but feature requests have been submitted
    to the NBIA team to make it possible to use this with the other series API endpoints.

    Chart width and height can be customized.
    """

    # format series_data into df depending on input_type
    df = formatSeriesInput(series_data, input_type = "", api_url = "")

    # Convert 'TimeStamp' column to datetime
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])

    # Filter out rows with missing timestamps
    df = df.dropna(subset=['TimeStamp'])

    # Group by 'Collection' and 'TimeStamp' (daily) and count unique 'SeriesInstanceUIDs'
    daily_data = df.groupby(['Collection', pd.Grouper(key='TimeStamp', freq='D')])['SeriesInstanceUID'].nunique().reset_index()

    # Calculate cumulative counts for each collection
    daily_data['CumulativeCount'] = daily_data.groupby('Collection')['SeriesInstanceUID'].cumsum()

    # Create a line chart using Plotly Express
    fig = px.line(
        daily_data,
        x='TimeStamp',
        y='CumulativeCount',
        color='Collection',
        labels={'CumulativeCount': 'Total Series'},
        title='Cumulative Total Series Over Time by Collection (Daily Aggregation)',
        markers='true'
    )

    # Customize the line thickness and chart size
    fig.update_xaxes(title='Release Date')
    fig.update_yaxes(title='Total Series')
    fig.update_traces(line=dict(width=4), marker=dict(size=15))
    fig.update_layout(width=chart_width, height=chart_height)

    # display figure
    fig.show()


def reportSeriesReleaseDate(series_data, chart_width = 1024, chart_height = 768):
    """
    Ingests the results of getSeries() as df or JSON and visualizes the
    release/publication timeline of the series in it by collection.

    Currently this only supports getSeries(), but feature requests have been submitted
    to the NBIA team to make it possible to use this with the other series API endpoints.

    Chart width and height can be customized.
    """

    # format series_data into df depending on input_type
    df = formatSeriesInput(series_data, input_type = "", api_url = "")

    # Convert 'DateReleased' column to datetime
    df['DateReleased'] = pd.to_datetime(df['DateReleased'])

    # Filter out rows with missing release dates
    df = df.dropna(subset=['DateReleased'])

    # Group by 'Collection' and 'DateReleased' (daily) and count unique 'SeriesInstanceUIDs'
    daily_data = df.groupby(['Collection', pd.Grouper(key='DateReleased', freq='D')])['SeriesInstanceUID'].nunique().reset_index()

    # Calculate cumulative counts for each collection
    daily_data['CumulativeCount'] = daily_data.groupby('Collection')['SeriesInstanceUID'].cumsum()

    # Create a line chart using Plotly Express
    fig = px.line(
        daily_data,
        x='DateReleased',
        y='CumulativeCount',
        color='Collection',
        labels={'CumulativeCount': 'Total Series'},
        title='Cumulative Total Series Over Time by Collection (Daily Aggregation)',
        markers='true'
    )

    # Customize the line thickness and chart size
    fig.update_xaxes(title='Release Date')
    fig.update_yaxes(title='Total Series')
    fig.update_traces(line=dict(width=4), marker=dict(size=15))
    fig.update_layout(width=chart_width, height=chart_height)

    # display figure
    fig.show()


def makeSeriesReport(series_data, input_type = "", format = "", filename = None, api_url = ""):
    """
    Ingests JSON output from any function that returns series-level data
    and creates summary report.
    If your series data is a dataframe instead of JSON you can use input_type = "df".
    Specify input_type = "manifest" to ingest a *.TCIA manifest file
    or "list" for a python list of UIDs.
    If input_type = "manifest" or "list" and there are series UIDs
    that are restricted, you must call getToken() with a user ID that
    has access to all UIDs before calling this function.
    Specifying api_url is only necessary if you are using
    input_type = "manifest" or "list" with NLST data (e.g. api_url = "nlst").
    Specify format = "var" to return the report values as a dictionary.
    Access variables example after saving function output
    to report_data: subjects = report_data["subjects"].
    Specify format = "file" to save the report to a file.
    Specify a filename parameter to set a filename
    if you don't want the default filename.
    """
    # format series_data into df depending on input_type
    df = formatSeriesInput(series_data, input_type, api_url)

    # Calculate summary statistics for a given collection

    # Scan Inventory
    subjects = len(df['PatientID'].value_counts())
    studies = len(df['StudyInstanceUID'].value_counts())
    series = len(df['SeriesInstanceUID'].value_counts())
    images = df['ImageCount'].sum()

    # Summarize Collections
    collections = df['Collection'].value_counts(dropna=False)

    # Summarize modalities
    modalities = df['Modality'].value_counts(dropna=False)

    # Summarize body parts
    body_parts = df['BodyPartExamined'].value_counts(dropna=False)

    # Summarize manufacturers
    manufacturers = df['Manufacturer'].value_counts(dropna=False)

    report = (
        # Scan Inventory
        f"Summary Statistics\n"
        f"Subjects: {subjects}\n"
        f"Studies: {studies}\n"
        f"Series: {series}\n"
        f"Images: {images}\n\n"

        # Summarize Collections
        f"Series Counts - Collections:\n"
        f"{collections}\n\n"

        # Summarize modalities
        f"Series Counts - Modality:\n"
        f"{modalities}\n\n"

        # Summarize body parts
        f"Series Counts - Body Parts Examined:\n"
        f"{body_parts}\n\n"

        # Summarize manufacturers
        f"Series Counts - Device Manufacturers:\n"
        f"{manufacturers}"
    )

    if format == "var":
        # Return the variables as a dictionary
        return {
            "subjects": subjects,
            "studies": studies,
            "series": series,
            "images": images,
            "collections": collections,
            "modalities": modalities,
            "body_parts": body_parts,
            "manufacturers": manufacturers
        }
    elif format == "file":
        if filename is None:
            # Generate default filename based on current date and time
            current_datetime = datetime.now().strftime("%Y%m%d-%H%M%S")
            filename = f"seriesReport-{current_datetime}.txt"

        # Save the report to a text file
        with open(filename, "w") as file:
            file.write(report)
        _log.info(f"Report saved to {filename}.")
    else:
        _log.info(report)


def reportDicomTags(series_uids: List[str], elements: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
    """
    Extract DICOM tags for a list of Series Instance UIDs.

    Args:
        series_uids (List[str]): A list of Series Instance UIDs to extract DICOM tags.
        elements (Optional[List[str]]): A list of elements to extract. If not specified, all elements
            are extracted. Elements should be specified with parentheses, e.g., ['(0018,0015)', '(0008,0060)'].

    Returns:
        Optional[pd.DataFrame]: A DataFrame containing the extracted DICOM tags with concatenated
        column headers, or None if no tags were found.
    """
    tag_summary_list = []

    for uid in series_uids:
        # Call getDicomTags to get tags for each UID
        tags = getDicomTags(uid, format="df")

        # Ensure tags is a valid DataFrame before processing
        if tags is None or not isinstance(tags, pd.DataFrame):
            _log.info(f"No valid tags found for UID {uid}. Skipping.")
            continue

        # Create a dictionary to store the extracted information with concatenated column headers
        extracted_info = {}
        extracted_info['Series Instance UID (0020,000E)'] = uid

        # Extract relevant information from tags based on elements
        if not elements:
            # Extract all elements
            for _, row in tags.iterrows():
                name = row.get('name')
                element = row.get('element')
                data = row.get('data')
                if name and element and name != 'Series Instance UID':
                    extracted_info[f'{name} {element}'] = data
        else:
            # Extract specific elements
            for element in elements:
                matching_rows = tags[tags['element'] == element]
                if not matching_rows.empty:
                    name = matching_rows['name'].values[0]
                    data = matching_rows['data'].values[0]
                    extracted_info[f'{name} {element}'] = data

        # Create a DataFrame for the current UID and extracted information
        tag_summary_df = pd.DataFrame(extracted_info, index=[0])

        # Append the DataFrame to the list
        tag_summary_list.append(tag_summary_df)

    if not tag_summary_list:
        _log.info("No results were returned.")
        return None

    tagSummary = pd.concat(tag_summary_list, ignore_index=True)
    return tagSummary


##########################
##########################
# Miscellaneous


def manifestToList(manifest):
    """
    Ingests a TCIA manifest file and removes header.
    Returns a list of series UIDs.
    """
    # initialize variable
    data = []

    # open file and write lines to a list
    with open(manifest) as f:
        # verify this is a tcia manifest file
        first_line = f.readline()
        f.seek(0, 0)
        if "downloadServerUrl" in first_line:
            _log.info("Removing headers from TCIA mainfest.")
            # write lines to list
            for line in f:
                data.append(line.rstrip())
            # remove the parameters from the list
            del data[:6]
            _log.info(f"Returning {len(data)} Series Instance UIDs (scans) as a list.")
            return data
        else:
            for line in f:
                data.append(line.rstrip())
            _log.warning(
                "This is not a TCIA manifest file, or you've already removed the header lines.\n"
                f"Returning {len(data)} Series Instance UIDs (scans) as a list."
            )
            return data

##########################
##########################
# Visualization 

def idcOhifViewer(data: Union[pd.DataFrame, List[dict]], max_rows: int = 500) -> Optional[HTML]:
    """
    Renders a DataFrame or JSON/list as an HTML table with clickable IDC OHIF viewer links.

    This utility function takes the output of a query (either a DataFrame or the
    default JSON/list format) and displays it as an interactive table. UIDs are
    converted into hyperlinks that open the IDC's OHIF viewer in a new tab.

    Args:
        data (Union[pd.DataFrame, List[dict]]): 
            The input data. This can be a Pandas DataFrame or a list of dictionaries
            (the default JSON output from functions like getSeries).
        max_rows (int, optional): 
            The maximum number of rows to display in the HTML table to prevent
            browser performance issues. Defaults to 500.

    Returns:
        Optional[IPython.display.HTML]: 
            An object that renders as a clickable HTML table in a notebook, or None
            if the input data is invalid.

    Example 1: Visualizing the default JSON output
        # Get data in the default format (JSON/list)
        series_json = nbia.getSeries(collection="TCGA-BRCA", modality="MR")
        
        # Directly visualize the result
        idcOhifViewer(series_json)

    Example 2: Visualizing a filtered DataFrame
        # Get the full dataset as a DataFrame
        all_series_df = nbia.getSeries(collection="TCGA-BRCA", modality="MR", format="df")
        
        # Filter for a specific patient
        patient_df = all_series_df[all_series_df['PatientID'] == 'TCGA-A2-A0CM']
        
        # Display the filtered results with IDC links
        idcOhifViewer(patient_df)
    """
    # --- 1. Handle different input types ---
    if isinstance(data, list):
        # Convert JSON/list output to a DataFrame
        df = pd.DataFrame(data)
    elif isinstance(data, pd.DataFrame):
        # The input is already a DataFrame
        df = data
    else:
        _log.error("Invalid input type. Please provide a Pandas DataFrame or a list (from JSON output).")
        return None
    
    if df.empty:
        _log.info("The provided DataFrame is empty. Nothing to display.")
        return

    # --- 2. Slice FIRST for performance, then process ---
    if len(df) > max_rows:
        _log.info(f"Displaying the first {max_rows} of {len(df)} rows.")
        # Use .copy() to avoid a potential SettingWithCopyWarning
        df_display = df.head(max_rows).copy()
    else:
        df_display = df.copy()

    # --- 3. Run the link-creation step on the smaller DataFrame ---
    base_url = "https://viewer.imaging.datacommons.cancer.gov/v3/viewer/"
    
    if 'SeriesInstanceUID' in df_display.columns and 'StudyInstanceUID' in df_display.columns:
        df_display['SeriesInstanceUID'] = df_display.apply(
            lambda row: f'<a href="{base_url}?StudyInstanceUIDs={row["StudyInstanceUID"]}&SeriesInstanceUIDs={row["SeriesInstanceUID"]}" target="_blank">{row["SeriesInstanceUID"]}</a>',
            axis=1
        )
    
    if 'StudyInstanceUID' in df_display.columns:
        df_display['StudyInstanceUID'] = df_display['StudyInstanceUID'].apply(
            lambda uid: f'<a href="{base_url}?StudyInstanceUIDs={uid}" target="_blank">{uid}</a>'
        )

    # --- 4. Convert to HTML and return for display ---
    html_output = df_display.to_html(escape=False, index=False)
    return HTML(html_output)

