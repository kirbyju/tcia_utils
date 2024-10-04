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
from datetime import datetime
from datetime import timedelta
from enum import Enum
import plotly.express as px
from tcia_utils.utils import searchDf
from tcia_utils.utils import copy_df_cols
from tcia_utils.utils import format_disk_space
from tcia_utils.utils import remove_html_tags
from tcia_utils.datacite import getDoi

class StopExecution(Exception):
    def _render_traceback_(self):
        pass


_log = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s'
    , level=logging.INFO
)


def log_request_exception(err: requests.exceptions.RequestException) -> int:
    if isinstance(err, requests.exceptions.HTTPError):
        if err.response.status_code == 401:
            _log.error(f"Authentication Error: {err}. Status code: 401. Unauthorized access.")
            return 401  # Unauthorized
        elif err.response.status_code == 403:
            _log.error(f"Permission Error: {err}. Status code: 403. Forbidden access.")
            return 403  # Forbidden
        elif err.response.status_code == 404:
            _log.error(f"Resource Not Found: {err}. Status code: 404.")
            return 404  # Not Found
        else:
            _log.error(f"HTTP Error: {err}. Status code: {err.response.status_code}")
            return 400  # Generic client error for other HTTP errors
    elif isinstance(err, requests.exceptions.ConnectionError):
        _log.error(f"Connection Error: {err}. Unable to reach the server.")
        return 502  # Bad Gateway or service down
    elif isinstance(err, requests.exceptions.Timeout):
        _log.error(f"Timeout Error: {err}. The request timed out.")
        return 408  # Timeout error
    else:
        _log.error(f"Request Exception: {err}. An unknown error occurred.")
        return 500  # General server error


# Used by functions that accept parameters used in GUI Simple Search
# e.g. getSimpleSearchWithModalityAndBodyPartPaged()
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

# Used by getSimpleSearchWithModalityAndBodyPartPaged() to transform
#   species codes into human readable terms
NPEXSpecies = {
    "human": 337915000
    , "mouse": 447612001
    , "dog": 448771007
}


def setApiUrl(endpoint, api_url):
    """
    setApiUrl() is used by most other functions to select the correct base URL
    and is generally not something that needs to be called directly in your code.

    It assists with:
        1. verifying you are calling a supported endpoint
        2. selecting the correct base URL for Search vs Advanced APIs
        3. selecting the correct base URL for regular collections vs NLST
        4. ensuring you have a valid security token where necessary

    Learn more about the NBIA APIs at https://wiki.cancerimagingarchive.net/x/ZoATBg
    """
    # create valid endpoint lists
    searchEndpoints = ["getCollectionValues", "getBodyPartValues", "getModalityValues",
                       "getPatient", "getPatientStudy", "getSeries", "getManufacturerValues",
                       "getSOPInstanceUIDs", "getSeriesMetaData", "getContentsByName",
                       "getImage", "getSingleImage", "getPatientByCollectionAndModality",
                       "NewPatientsInCollection", "NewStudiesInPatientCollection",
                       "getSeriesSize", "getUpdatedSeries"]
    advancedEndpoints = ["getModalityValuesAndCounts", "getBodyPartValuesAndCounts",
                         "getDicomTags", "getSeriesMetadata2", "getCollectionOrSeriesForDOI",
                         "getCollectionValuesAndCounts", "getCollectionDescriptions",
                         "getSimpleSearchWithModalityAndBodyPartPaged", "getManufacturerValuesAndCounts",
                         "getAdvancedQCSearch", "createSharedList"]

    if endpoint not in searchEndpoints and endpoint not in advancedEndpoints:
        _log.error(
            f"Endpoint not supported by tcia_utils: {endpoint}\n"
            f'Valid "Search" endpoints include {searchEndpoints}\n'
            f'Valid "Advanced" endpoints include {advancedEndpoints}'
        )
        raise StopExecution

    # ensure a token exists
    if api_url == "nlst":
        if 'nlst_token_exp_time' not in globals():
            getToken(user="nbia_guest", api_url="nlst")
        if 'nlst_token_exp_time' in globals() and datetime.now() > nlst_token_exp_time:
            refreshToken(api_url = "nlst")
    else:
        if 'token_exp_time' not in globals():
            getToken(user="nbia_guest")
            _log.info("Accessing public data anonymously. To access restricted data use nbia.getToken() with your credentials.")
        if 'token_exp_time' in globals() and datetime.now() > token_exp_time:
            refreshToken()

    if api_url in ["", "restricted"]:
        base_url = "https://services.cancerimagingarchive.net/nbia-api/services/v2/" if endpoint in searchEndpoints else "https://services.cancerimagingarchive.net/nbia-api/services/"
    elif api_url == "nlst":
        base_url = "https://nlst.cancerimagingarchive.net/nbia-api/services/v2/" if endpoint in searchEndpoints else "https://nlst.cancerimagingarchive.net/nbia-api/services/"
    else:
        _log.error(
            f'"{api_url}" is an invalid api_url for the {"Search" if endpoint in searchEndpoints else "Advanced"} API endpoint: {endpoint}'
        )
        raise StopExecution

    return base_url


def getToken(user="", pw="", api_url=""):
    """
    getToken() accepts user and pw parameters to create a token to access APIs that require authorization.
    Access tokens can be refreshed with refreshToken().
    Set user = "nbia_guest" for anonymous access to Advanced API functions
    Interactive prompts are provided for user/pw if they're not specified as parameters.
    "Advanced APIs" can be accessed anonymously using the nbia_guest account with the default guest password.
    Set api_url to "nlst" for accessing the NLST server.
    """
    global token_exp_time, api_call_headers, access_token, refresh_token, id_token, nlst_token_exp_time, nlst_api_call_headers, nlst_access_token, nlst_refresh_token, nlst_id_token

    # specify user/pw unless nbia_guest is being used for accessing Advanced API anonymously
    if user != "":
        userName = user
    else:
        print("Enter User: ")
        userName = input()
    # set password for non-guest logins
    if userName == "nbia_guest":
        passWord = "ItsBetweenUAndMe" # this guest account password is documented in the public API guide
    elif pw == "":
        passWord = getpass.getpass(prompt='Enter Password: ')
    else:
        passWord = pw

    # request API token
    try:
        params = {'client_id': 'nbia',
                  'scope': 'openid',
                  'grant_type': 'password',
                  'username': userName,
                  'password': passWord
                 }

        if api_url == "nlst":
            token_url = "https://keycloak.dbmi.cloud/auth/realms/TCIA/protocol/openid-connect/token"
        else:
            token_url = "https://keycloak-stg.dbmi.cloud/auth/realms/TCIA/protocol/openid-connect/token"
        data = requests.post(token_url, data=params)
        data.raise_for_status()
        tmp_access_token = data.json()["access_token"]
        expires_in = data.json()["expires_in"]
        tmp_id_token = data.json()["id_token"]
        # track expiration status/time
        current_time = datetime.now()
        tmp_token_exp_time = current_time + timedelta(seconds=expires_in)
        tmp_api_call_headers = {'Authorization': 'Bearer ' + tmp_access_token}
        tmp_refresh_token = data.json()["refresh_token"]

        # Store tokens separately for each server
        if api_url == "nlst":
            nlst_access_token = tmp_access_token
            nlst_token_exp_time = tmp_token_exp_time
            nlst_api_call_headers = tmp_api_call_headers
            nlst_refresh_token = tmp_refresh_token
            nlst_id_token = tmp_id_token
            _log.info(f'Success - Token saved to nlst_api_call_headers variable and expires at {nlst_token_exp_time}')
        else:
            access_token = tmp_access_token
            token_exp_time = tmp_token_exp_time
            api_call_headers = tmp_api_call_headers
            refresh_token = tmp_refresh_token
            id_token = tmp_id_token
            _log.info(f'Success - Token saved to api_call_headers variable and expires at {token_exp_time}')
        return 200
    # handle errors
    except requests.exceptions.RequestException as err:
        return log_request_exception(err)
        raise StopExecution


def refreshToken(api_url="primary"):
    """
    refreshToken() refreshes security tokens to extend access time for APIs
    that require authorization. It attempts to verify that a refresh token
    exists and recommends using getToken() to create a new token if needed.
    This function is called as needed by setApiUrl() and is generally not
    something that needs to be called directly in your code.
    """
    global token_exp_time, api_call_headers, access_token, refresh_token, id_token
    global nlst_token_exp_time, nlst_api_call_headers, nlst_access_token, nlst_refresh_token, nlst_id_token

    # copy the relevant token to refresh (nlst vs primary server) into tmp_token
    try:
        if api_url == "nlst":
            tmp_token = nlst_refresh_token
        else:
            tmp_token = refresh_token
    except NameError:
        _log.error("No token found. Create one using getToken().")
        raise StopExecution

    # refresh token request
    try:
        params = {
            'client_id': 'nbia',
            'grant_type': 'refresh_token',
            'refresh_token': tmp_token
        }

        if api_url == "nlst":
            token_url = "https://keycloak.dbmi.cloud/auth/realms/TCIA/protocol/openid-connect/token"
        else:
            token_url = "https://keycloak-stg.dbmi.cloud/auth/realms/TCIA/protocol/openid-connect/token"
        response = requests.post(token_url, data=params)
        response.raise_for_status()
        data = response.json()
        tmp_access_token = data.get("access_token")
        expires_in = data.get("expires_in")
        tmp_id_token = data.get("id_token")

        if not tmp_access_token or not expires_in:
            _log.error("Failed to refresh access token.")

        # track expiration status/time
        current_time = datetime.now()
        tmp_token_exp_time = current_time + timedelta(seconds=expires_in)
        tmp_api_call_headers = {'Authorization': 'Bearer ' + tmp_access_token}
        tmp_refresh_token = data.get("refresh_token")

        # Store tokens separately for each server
        if api_url == "nlst":
            nlst_access_token = tmp_access_token
            nlst_token_exp_time = tmp_token_exp_time
            nlst_api_call_headers = tmp_api_call_headers
            nlst_refresh_token = tmp_refresh_token
            nlst_id_token = tmp_id_token
            _log.info(f'Success - Token refreshed for nlst_api_call_headers variable and expires at {nlst_token_exp_time}')
        else:
            access_token = tmp_access_token
            token_exp_time = tmp_token_exp_time
            api_call_headers = tmp_api_call_headers
            refresh_token = tmp_refresh_token
            id_token = tmp_id_token
            _log.info(f'Success - Token refreshed for api_call_headers variable and expires at {token_exp_time}')
        return 200

    # handle errors
    except requests.exceptions.RequestException as err:
        return log_request_exception(err)
        raise StopExecution


def makeCredentialFile(user = "", pw = ""):
    """
    Creates a credential file to use with NBIA Data Retriever.
    Interactive prompts are provided for user/pw if they're not specified as parameters.
    The credential file is a text file that passes the user's credentials in the following format:
        userName = YourUserName
        passWord = YourPassword
        Both parameters are case-sensitive.
    Additional documentation:
        https://wiki.cancerimagingarchive.net/x/2QKPBQ
        https://github.com/kirbyju/TCIA_Notebooks/blob/main/TCIA_Linux_Data_Retriever_App.ipynb
    """
    # set user name and password
    if user == "":
        print("Enter User: ")
        userName = input()
    else:
        userName = user
    if pw == "":
        passWord = getpass.getpass(prompt = 'Enter Password: ')
    else:
        passWord = pw

    # create credential file to use with NBIA Data Retriever
    lines = ['userName=' + userName, 'passWord=' + passWord]
    with open('credentials.txt', 'w') as f:
        f.write('\n'.join(lines))
    _log.info("Credential file for NBIA Data Retriever saved: credentials.txt")


def queryData(endpoint, options, api_url, format, method="GET", param=None):
    """
    queryData() is called by many other query functions and is generally
    not something that needs to be called directly in your code.
    It uses setApiURL() to set a base URL and handles errors
    for HTTP status and empty search results.
    Formats output as JSON by default with options for "df" (dataframe) and "csv".
    """
    base_url = setApiUrl(endpoint, api_url)
    url = f"{base_url}{endpoint}"
    response = None

    try:
        headers = nlst_api_call_headers if api_url == "nlst" else api_call_headers

        if method.upper() == "POST":
            _log.info(f'Calling {endpoint} with parameters {param}')
            response = requests.post(url, headers=headers, data=param)
        else:
            _log.info(f'Calling {endpoint} with parameters {options}')
            response = requests.get(url, params=options, headers=headers)

        response.raise_for_status()

        if response and not response.content.strip():
            _log.info(f"No results found.")
            return None

    except requests.exceptions.RequestException as err:
        return log_request_exception(err)
    except ValueError as json_err:
        _log.error(f"JSON Decode Error: {json_err} - Response text: {response.text if response else 'No response'}")
        return None

    else:
        data = response.json()
        # format the output
        if format == "df":
            df = pd.DataFrame(data)
            return df
        elif format == "csv":
            df = pd.DataFrame(data)
            csv_filename = f"{endpoint}_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv"
            df.to_csv(csv_filename)
            _log.info(f"CSV saved to: {csv_filename}")
            return df
        else:
            return data


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

    # create options dict to construct URL
    options = {}
    options['Collection'] = collection
    options['Date'] = date

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

    # create options dict to construct URL
    options = {}
    options['Collection'] = collection
    options['PatientID'] = patientId
    options['Date'] = date

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

    # convert to NBIA's expected date format
    # It appears there is likely a bug in the API here.
    # Date format for the API is currently DD/MM/YYYY so we'll convert it.
    nbiaDate = datetime.strptime(date, "%Y/%m/%d").strftime("%d/%m/%Y")


    # create options dict to construct URL
    options = {}
    options['fromDate'] = nbiaDate

    data = queryData(endpoint, options, api_url, format)
    return data

def getSeriesMetadata(seriesUid,
                      api_url = "",
                      format = ""):
    """
    Optional: api_url, format
    Gets Series (scan) metadata from a specified api_url.
    Output includes DOI and license details that were historically
      not in the getSeries() function.
    """
    endpoint = "getSeriesMetaData"

    # create options dict to construct URL
    options = {}
    options['SeriesInstanceUID'] = seriesUid

    data = queryData(endpoint, options, api_url, format)
    return data

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


def downloadSeries(series_data: Union[str, pd.DataFrame, List[str]],
                   number: int = 0,
                   path: str = "",
                   hash: str = "",
                   api_url: str = "",
                   input_type: str = "",
                   format: str = "",
                   csv_filename: str = "",
                   as_zip: bool = False) -> Union[pd.DataFrame, None]:
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
    endpoint = "getImage"
    success = 0
    failed = 0
    previous = 0

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

    # Set sample size if you don't want to download the full set of results
    _log.info(f"Downloading {number if number > 0 else len(series_data)} out of {len(series_data)} Series Instance UIDs (scans).")

    # Set option to include md5 hashes
    downloadOptions = "getImageWithMD5Hash?SeriesInstanceUID=" if hash == "y" else "getImage?NewFileNames=Yes&SeriesInstanceUID="

    # Get the data
    try:
        for seriesUID in series_data:
            pathTmp = os.path.join(path, seriesUID) if path else os.path.join("tciaDownload", seriesUID)
            zip_path = f"{pathTmp}.zip"
            base_url = setApiUrl(endpoint, api_url)
            headers = nlst_api_call_headers if api_url == "nlst" else api_call_headers
            metadata_url = base_url + "getSeriesMetaData?SeriesInstanceUID=" + seriesUID

            # Check for previously downloaded data
            if not os.path.isdir(pathTmp) and not os.path.isfile(zip_path):
                data_url = base_url + downloadOptions + seriesUID

                # Download data
                _log.info(f"Downloading... {data_url}")
                data = requests.get(data_url, headers=headers)
                if data.status_code == 200:
                    # If `as_zip` is True, save the response as a zip file and don't extract it
                    if as_zip:
                        with open(zip_path, "wb") as zip_file:
                            zip_file.write(data.content)
                        success += 1
                    else:
                        # Unzip file
                        with zipfile.ZipFile(io.BytesIO(data.content)) as file:
                            file.extractall(path=pathTmp)
                        success += 1
                    # Get metadata if desired
                    if manifestDF is not None:
                        metadata = requests.get(metadata_url, headers=headers).json()
                        manifestDF = pd.concat([manifestDF, pd.DataFrame(metadata)], ignore_index=True)
                    if number > 0 and success == number:
                        break
                else:
                    _log.error(f"Error: {data.status_code} Series failed: {seriesUID}")
                    failed += 1
            else:
                if os.path.isdir(pathTmp):
                    _log.warning(f"Series {seriesUID} already downloaded and unzipped.")
                elif os.path.isfile(zip_path):
                    _log.warning(f"Series {seriesUID} already downloaded as a zip file.")
                if manifestDF is not None:
                    metadata = requests.get(metadata_url, headers=api_call_headers).json()
                    manifestDF = pd.concat([manifestDF, pd.DataFrame(metadata)], ignore_index=True)
                previous += 1

        # Summarize download results
        _log.info(
            f"Downloaded {success} out of {number if number > 0 else len(series_data)} Series Instance UIDs (scans).\n"
            f"{failed} failed to download.\n"
            f"{previous} previously downloaded."
        )

    except requests.exceptions.RequestException as err:
        return log_request_exception(err)

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
            headers = nlst_api_call_headers if api_url == "nlst" else api_call_headers
            data = requests.get(data_url, headers=headers)

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


def getSeriesList(uids, api_url = "", csv_filename = "", format = ""):
    """
    Get metadata for a list of series from Advanced API.
    Returns result as dataframe (default) or as CSV if format = 'csv'.
    Use csv_filename to set a custom filename.
    """

    # break up the list into smaller chunks if > 10,000 series
    chunk_size = 10000
    if len(uids) > chunk_size:
        chunked_uids = list()
        for i in range(0, len(uids), chunk_size):
            chunked_uids.append(uids[i:i+chunk_size])
        # Count how many chunks
        chunk_count = len(chunked_uids)
        _log.info(f'Your data has been split into {chunk_count} groups.')
    else:
        chunk_count = 0


    if chunk_count == 0:
        df = getSeriesListData(uids, api_url)
    else:
        count = 0
        dfs = []  # create an empty list to store DataFrames
        for x in chunked_uids:
            str_count = str(count)
            chunk_df = getSeriesListData(x, api_url)
            dfs.append(chunk_df)  # append the DataFrame for this chunk to the list
            count += 1

        # concatenate all the DataFrames in the list into a single DataFrame
        df = pd.concat(dfs, ignore_index=True)

    if format == "csv" and csv_filename != "":
        df.to_csv(csv_filename + '.csv')
        _log.info(f"Report saved as {csv_filename}.csv")
    elif format == "csv":
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'series_report_{timestamp}.csv'
        df.to_csv(filename, index=False)
        _log.info(f"Collection summary report saved as '{filename}'")
    else:
        return df


def getSeriesListData(uids, api_url):
    """
    Ingests input from getSeriesList().
    Not intended to be used directly.
    Note: API returns a CSV (not JSON) which this converts to df.
    """
    uidList = ",".join(uids)
    param = {'list': uidList}
    endpoint = "getSeriesMetadata2"

    base_url = setApiUrl(endpoint, api_url)
    url = f"{base_url}{endpoint}"
    response = None

    try:
        headers = nlst_api_call_headers if api_url == "nlst" else api_call_headers
        _log.info(f'Calling {endpoint} with parameters {param}')
        response = requests.post(url, headers=headers, data=param)
        response.raise_for_status()

        if response and not response.content.strip():
            _log.info(f"No results found.")
            return None

    except requests.exceptions.RequestException as err:
        return log_request_exception(err)
        return None

    else:
        df = pd.read_csv(io.StringIO(response.text), sep=',')
        return df


def getDicomTags(seriesUid,
                 api_url = "",
                 format = ""):
    """
    Gets DICOM tag metadata for a given series UID (scan)
    """
    endpoint = "getDicomTags"

    # create options dict to construct URL
    options = {}
    options['SeriesUID'] = seriesUid

    data = queryData(endpoint, options, api_url, format)
    return data


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


def getDoiMetadata(doi, output="", api_url="", format=""):
    """
    Optional: output, api_url, format
    Gets a list of Collections if output = "", or Series UIDs if output = "series", associated with a DOI.
    The result includes whether the data are 3rd party analyses or not.
    """
    param = {'DOI': doi, 'CollectionOrSeries': output}
    endpoint = "getCollectionOrSeriesForDOI"

    # Use queryData to make the POST request
    data = queryData(endpoint=endpoint, options = None, api_url=api_url, format=format, method="POST", param=param)

    return data


def getSimpleSearchWithModalityAndBodyPartPaged(
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
    Note: This function only supports output of JSON format, please leave the format parameter as it.

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

    Example call: getSimpleSearchWithModalityAndBodyPartPaged(collections=["TCGA-UCEC", "4D-Lung"], modalities=["CT"])
    """

    endpoint = "getSimpleSearchWithModalityAndBodyPartPaged"
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
    if fromDate and toDate:
        criteria = getCriteria()
        options[criteria] = Criteria.DateRange
        options["fromDate" + str(criteriaTypeIndex)] = fromDate
        options["toDate" + str(criteriaTypeIndex)] = toDate
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
        metadata = requests.post(url, headers = api_call_headers, data = options)
        metadata.raise_for_status()

        # check for empty results and format output
        if metadata.text and metadata.text != "[]":
            metadata = metadata.json()
            # format the output (optional)
            if format == "df":
                df = pd.DataFrame(metadata)
                return df
            elif format == "csv":
                df = pd.DataFrame(metadata)
                df.to_csv(endpoint + ".csv")
                _log.info("CSV saved to: " + endpoint + ".csv")
                return df
            else:
                return metadata
        else:
            _log.info("No results found.")

    except requests.exceptions.RequestException as err:
        return log_request_exception(err)


def getAdvancedQCSearch(criteria_values, api_url = "", format = "", input_type = {}):
    """
    This function allows TCIA data curators to perform an advanced QC search.
    This function will not work for end users of TCIA.

    Parameters:
    - criteria_values (list of tuples): A list of tuples where each tuple contains a criteria type and its corresponding value.
        - For example, [("patientID", "12345"), ("studyUID", "67890")].
        - The criteria type can be one of the following: "collection", "qcstatus", "released", "batchnumber", "complete", "patientID", "submissiondate", "studyUID", "seriesUID", "studyDate", "seriesDesc", "modality", "manufacturer".
        - The value depends on the criteria type and the input type. For "list" input type, it can be a single value or multiple values separated by commas. For "commaSeperatedList" input type, it should be a list of exact matches. For "contains" input type, it should be a substring to search for. For "dateRange" input type, it should be a date or a range of dates in the format "MM/DD/YYYY".
    - api_url (str, optional): The base URL for the API. Defaults to an empty string.
    - format (str, optional): The format of the response. Defaults to an empty string.
    - override_input_type (dict, optional): A dictionary where keys are criteria types and values are the new input types. This allows the user to override the default input type for certain criteria types. Defaults to an empty dictionary.
        - For example, {"patientID": "contains", "studyUID": "contains"}.

    Returns:
    - data: The response from the API call.

    Examples:

    1. Searching for all patient IDs that contain “LIDC”, have a modality of “CR” or “DX”,
    and a submission date between Feb 25, 2020 and Jan 1, 2023. Uses input_type to override
    the default for patientID (comma separated list) to return anything that contains "LIDC".

        criteria_values = [("patientID", "LIDC"),
                            ("modality", "CR,DX"),
                            ("submissiondate", "2/25/2020"),
                            ("submissiondate", "1/1/2023")]

        input_type = {"patientID": "contains"}

        getAdvancedQCSearch(criteria_values, input_type=input_type)

    2. Searching for all data with collection “APOLLO-5-KIRP//UVA-Limited” or “APOLLO-5-LIHC//UVA-Limited”
    and qcstatus is either “Not Visible” or “Visible” with results formatted as a dataframe.

        criteria_values = [("collection", "APOLLO-5-KIRP//UVA-Limited"),
                            ("collection", "APOLLO-5-LIHC//UVA-Limited"),
                            ("qcstatus", "Not Visible"),
                            ("qcstatus", "Visible")]

        getAdvancedQCSearch(criteria_values, format = "df")

    3. Searching for a list of UIDs with results formatted as a dataframe and saved to a CSV file.

        criteria_values = [("seriesUID", "1.3.6.1.4.1.14519.5.2.1.6279.6001.709632090821449989953075380984,1.3.6.1.4.1.14519.5.2.1.6279.6001.109097931021726413867023009234")]

        getAdvancedQCSearch(criteria_values, format = "csv")
    """
    # Mapping between criteriaType and inputType
    input_type_map = {
        "qcstatus": "list",
        "released": "list",
        "batchnumber": "list",
        "complete": "list",
        "patientID": "commaSeperatedList",
        "submissiondate": "dateRange",
        "studyUID": "commaSeperatedList",
        "seriesUID": "commaSeperatedList",
        "studyDate": "dateRange",
        "seriesDesc": "contains",
        "modality": "list",
        "manufacturer": "list"
    }

    endpoint = "getAdvancedQCSearch"

    param = {}
    for i, (criteria, value) in enumerate(criteria_values):
        param[f"criteriaType{i}"] = criteria
        # Check if the criteria has an override input type
        if criteria in input_type:
            param[f"inputType{i}"] = input_type[criteria]
        else:
            param[f"inputType{i}"] = input_type_map[criteria]
        param[f"boolean{i}"] = "AND"
        param[f"value{i}"] = value

    # Use queryData to make the POST request
    data = queryData(endpoint=endpoint, options = None, api_url=api_url, format=format, method="POST", param=param)

    return data


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
        df = getSeriesList(series_data, api_url = api_url)
    # pass the dataframe through if one was provided
    elif input_type == "df":
        df = series_data
    # create a dataframe if json was provided
    else:
        # Create a DataFrame from the series_data
        df = pd.DataFrame(series_data)

    # Rename the headers
    if df is None or df.empty:
        _log.warning(f"No data was provided for reformatting.")
        raise StopExecution
    else:
        column_mapping = {
                'Collection Name': 'Collection',
                'Data Description URI': 'CollectionURI',
                # Modality is the same
                # Manufacturer is the same
                'Body Part Examined': 'BodyPartExamined',
                'Subject ID': 'PatientID',
                'Study UID': 'StudyInstanceUID',
                'Study Description': 'StudyDesc',
                'Study Date': 'StudyDate',
                'Series ID': 'SeriesInstanceUID',
                'Series Description': 'SeriesDescription',
                'Series Number': 'SeriesNumber',
                'Protocol Name': 'ProtocolName',
                'Series Date': 'SeriesDate',
                'Number of images': 'ImageCount',
                'File Size (Bytes)': 'FileSize',
                #TimeStamp is the same
                'Date Released': 'DateReleased',
                '3rd Party Analysis': 'ThirdPartyAnalysis',
                'Manufacturer Model Name': 'ManufacturerModelName',
                'Software Versions': 'SoftwareVersions',
                'License Name': 'LicenseName',
                'License URL': 'LicenseURI',
                'Annotations Flag': 'AnnotationsFlag'
            }

        # Renaming the columns in the DataFrame
        df.rename(columns=column_mapping, inplace=True)

        # Verify all columns exist that should be there
        required_columns = list(column_mapping.values()) + ['Modality', 'Manufacturer', 'TimeStamp']

        for col in required_columns:
                if col not in df.columns:
                    df[col] = None

        # Make all URLs lower case
        df['CollectionURI'] = df['CollectionURI'].str.lower()
        df['LicenseURI'] = df['LicenseURI'].str.lower()

        # Format date-related columns to datetime
        df['DateReleased'] = pd.to_datetime(df['DateReleased'])
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])

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
        group = "CollectionURI"
        column = "Collection"
        columnGrouped = "Collections"
        chartLabel = "Identifier"
    else:
        group = "Collection"
        column = "CollectionURI"
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

        # Extract DOI from the end of CollectionURI and store it in "DOI" column
        grouped["DOI"] = grouped["CollectionURI"].str.extract(r'doi.org/(\S+)$')

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


def makeSharedCart(uids, name, description, description_url, api_url=""):
    """
    Create a shared cart from a list of series UIDs.
    """
    # Construct the query string manually
    uid_query = "&".join([f"list={uid}" for uid in uids])
    param = f"{uid_query}&name={name}&description={description}&url={description_url}"
    endpoint = "createSharedList"

    # set urls
    base_url = setApiUrl(endpoint, api_url)
    url = base_url + endpoint

    # get data & handle any request.post() errors
    try:
        headers = nlst_api_call_headers if api_url == "nlst" else api_call_headers
        # Create a copy of headers and add the new header
        headers_with_content_type = headers.copy()
        headers_with_content_type['Content-Type'] = 'application/x-www-form-urlencoded'
        _log.info(f'Calling {endpoint} with parameters {param}')
        metadata = requests.post(url, headers=headers_with_content_type, data=param)
        metadata.raise_for_status()
        # log success and return success code
        _log.info(metadata.text)
        return 200

    except requests.exceptions.RequestException as err:
        return log_request_exception(err)


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


def reportDicomTags(series_uids, elements=None):
    """
    Extract DICOM tags for a list of Series Instance UIDs.

    Args:
    - series_uids: A list of Series Instance UIDs to extract DICOM tags.
    - elements: (optional) A list of elements to extract. If not specified, all elements are extracted.
                Elements should be specified with parentheses, e.g. ['(0018,0015)', '(0008,0060)'].

    Returns:
    - A DataFrame containing the extracted DICOM tags with concatenated column headers.
    """
    # Initialize an empty list to store DataFrames
    tag_summary_list = []

    for uid in series_uids:
        # Call nbia.getDicomTags to get tags for each uid
        tags = getDicomTags(uid, format="df")

        if tags is None:
            _log.info(f"No tags found for: {uid}")
        else:
            # Create a dictionary to store the extracted information with concatenated column headers
            extracted_info = {}

            # Add the 'Series Instance UID' column
            extracted_info['Series Instance UID (0020,000E)'] = uid

            # Extract the relevant information from tags based on elements
            if not elements:
                # Extract all elements
                for index, row in tags.iterrows():
                    name = row['name']
                    element = row['element']
                    if name != 'Series Instance UID':
                        extracted_info[f'{name} {element}'] = row['data']
            else:
                # Extract specific elements
                for element in elements:
                    matching_rows = tags[tags['element'] == element]
                    if not matching_rows.empty:
                        # Use the 'name' and 'data' columns with concatenated column headers
                        name = matching_rows['name'].values[0]
                        element = matching_rows['element'].values[0]
                        extracted_info[f'{name} {element}'] = matching_rows['data'].values[0]

            # Create a DataFrame for the current uid and extracted information
            tag_summary_df = pd.DataFrame(extracted_info, index=[0])

            # Append the DataFrame to the list
            tag_summary_list.append(tag_summary_df)

    # Check if tag_summary_list is empty
    if not tag_summary_list:
        _log.info(f"No results were returned.")
        return None
    else:
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

def makeVizLinks(series_data, csv_filename=""):
    """
    Ingests JSON output of getSeries() or getSharedCart().
    Creates URLs to visualize them in a browser.
    The links appear in the last 2 columns of the dataframe.
    TCIA links display the individual series described in each row.
    IDC links display the entire study (all scans from that time point).
    IDC links may not work if they haven't mirrored the series from TCIA, yet.
    This function only works with fully public datasets (not limited-access).
    Accepts a csv_filename parameter if you'd like to export a CSV file.
    """
    # set base urls for tcia/idc
    tciaVizUrl = "https://nbia.cancerimagingarchive.net/viewer/?series="
    idcVizUrl = "https://viewer.imaging.datacommons.cancer.gov/viewer/"

    # create dataframe and append base URLs to study/series UIDs
    df = pd.DataFrame(series_data)
    df['VisualizeSeriesOnTcia'] = tciaVizUrl + df['SeriesInstanceUID']
    df['VisualizeStudyOnIdc'] = idcVizUrl + df['StudyInstanceUID']

    # display manifest dataframe and/or save manifest to CSV file
    if csv_filename != "":
        df.to_csv(csv_filename + '.csv')
        _log.info(f"Manifest CSV saved as {csv_filename}.csv")
        return df
    else:
        return df


def viewSeries(*args, **kwargs):
    """
    This function has been removed from `tcia_utils`.

    The functionality is now available in the `simpleDicomViewer` package.

    Please follow the instructions below to install the necessary dependencies:

    1. Install the forked `pydicom-seg` with updated jsonschema version:

        import sys
        !{sys.executable} -m pip install --upgrade -q git+https://github.com/kirbyju/pydicom-seg.git@master

    2. Install `simpleDicomViewer`:

        import sys
        !{sys.executable} -m pip install --upgrade -q simpleDicomViewer

    Usage in `simpleDicomViewer` has changed slightly as the `seriesUid` parameter is no longer available:

        from simpleDicomViewer import viewSeries
        viewSeries(path = "")
    """
    raise NotImplementedError("viewSeries() has been migrated to the `simpleDicomViewer` PyPI package.")


def viewSeriesAnnotation(*args, **kwargs):
    """
    This function has been removed from `tcia_utils`.

    The functionality is now available in the `simpleDicomViewer` package.

    Please follow the instructions below to install the necessary dependencies:

    1. Install the forked `pydicom-seg` with updated jsonschema version:

        import sys
        !{sys.executable} -m pip install --upgrade -q git+https://github.com/kirbyju/pydicom-seg.git@master

    2. Install `simpleDicomViewer`:

        import sys
        !{sys.executable} -m pip install --upgrade -q simpleDicomViewer

    Usage in `simpleDicomViewer` has changed slightly as the `seriesUid` and annotationUid parameters are no longer available.

    Note that annotationPath should be the path to the specific segmentation file name as opposed to a directory containing multiple segmentation files:

        from simpleDicomViewer import viewSeriesAnnotations
        viewSeriesAnnotation(seriesPath = "", annotationPath = "")
    """
    raise NotImplementedError("viewSeriesAnnotation() has been migrated to the `simpleDicomViewer` PyPI package.")
