####### setup
import logging
import requests
import pandas as pd
import getpass
import zipfile
import io
import os
from datetime import datetime
from datetime import timedelta
from enum import Enum
import matplotlib
import matplotlib.pyplot as plt
import plotly.express as px
import pydicom
import numpy as np
from ipywidgets import interact
from tcia_utils.utils import searchDf


class StopExecution(Exception):
    def _render_traceback_(self):
        pass


_log = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s'
    , level=logging.INFO
)

# set token creation URL for getToken and refreshToken
token_url = "https://keycloak.dbmi.cloud/auth/realms/TCIA/protocol/openid-connect/token"

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
    global searchEndpoints, advancedEndpoints

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
                         "getSimpleSearchWithModalityAndBodyPartPaged", "getManufacturerValuesAndCounts"]

    if not endpoint in searchEndpoints and not endpoint in advancedEndpoints:
        _log.error(
            f"Endpoint not supported by tcia_utils: {endpoint}\n"
            f'Valid "Search" endpoints include {searchEndpoints}\n'
            f'Valid "Advanced" endpoints include {advancedEndpoints}'
        )
        raise StopExecution
    else:
        # set base URL for simple search and nlst simple search (no login required)
        if api_url == "":
            if endpoint in searchEndpoints:
                # Using "Search" API (no login required): https://wiki.cancerimagingarchive.net/x/fILTB
                base_url = "https://services.cancerimagingarchive.net/nbia-api/services/v1/"
            if endpoint in advancedEndpoints:
                # Using "Advanced" API (login required): https://wiki.cancerimagingarchive.net/x/YoATBg
                # check if valid token exists, use anonymous login if not
                if 'token_exp_time' not in globals():
                    getToken(user = "nbia_guest")
                    _log.info("Accessing Advanced API anonymously. To access restricted data use nbia.getToken() with your credentials.")
                if 'token_exp_time' in globals() and datetime.now() > token_exp_time:
                    refreshToken()
                base_url = "https://services.cancerimagingarchive.net/nbia-api/services/"
        elif api_url == "nlst":
            if endpoint in searchEndpoints:
                # Using "Search" API with NLST server (no login required): https://wiki.cancerimagingarchive.net/x/fILTB
                base_url = "https://nlst.cancerimagingarchive.net/nbia-api/services/v1/"
            if endpoint in advancedEndpoints:
                # Using "Advanced" API docs (login required): https://wiki.cancerimagingarchive.net/x/YoATBg
                # Checking to see if a valid NLST authentication token exists
                if 'token_exp_time' not in globals():
                    getToken(user = "nbia_guest")
                if 'token_exp_time' in globals() and datetime.now() > token_exp_time:
                    refreshToken()
                base_url = "https://nlst.cancerimagingarchive.net/nbia-api/services/"
        elif api_url == "restricted":
            if endpoint in searchEndpoints:
                # Using "Search with Authentication" API (login required): https://wiki.cancerimagingarchive.net/x/X4ATBg
                # Checking to see if a valid authentication token exists
                if 'token_exp_time' not in globals():
                    _log.error("Error using token for accessing the Restricted API. Create one using getToken().")
                    raise StopExecution
                if 'token_exp_time' in globals() and datetime.now() > token_exp_time:
                    refreshToken()
                base_url = "https://services.cancerimagingarchive.net/nbia-api/services/v2/"
            if endpoint in advancedEndpoints:
                _log.error(
                    f'"{api_url}" is an invalid api_url for the Advanced API endpoint: {endpoint}\n'
                    "Remove the api_url parameter unless you are querying the National Lung Screening Trial collection.\n"
                    "Use api_url = \"nlst\" to query the National Lung Screening Trial collection."
                )
                raise StopExecution
        else:
            if endpoint in searchEndpoints:
                _log.error(
                    f'"{api_url}" is an invalid api_url for the Search API endpoint: {endpoint}\n'
                    "Remove the api_url parameter for regular public dataset searches.\n"
                    'Use api_url = "nlst" to access the National Lung Screening Trial collection.\n'
                    'Use api_url = "restricted" to access collections that require logging in.'
                )
                raise StopExecution
            if endpoint in advancedEndpoints:
                _log.error(
                    f'"{api_url}" is an invalid api_url for the Advanced API endpoint: {endpoint}\n'
                    "Remove the api_url parameter unless you are querying the National Lung Screening Trial collection.\n"
                    'Use api_url = "nlst" to query the National Lung Screening Trial collection.'
                )
                raise StopExecution

        return base_url


def getToken(user="", pw=""):
    """
    getToken() accepts user and pw parameters to create a token to access APIs that require authorization.
    Access tokens can be refreshed with refreshToken().
    Set user = "nbia_guest" for anonymous access to Advanced API functions
    Interactive prompts are provided for user/pw if they're not specified as parameters.
    "Advanced APIs" can be accessed anonymously using the nbia_guest account with the default guest password.
    """
    global token_exp_time, api_call_headers, access_token, refresh_token, id_token

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
        passWord = getpass.getpass(prompt = 'Enter Password: ')
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

        data = requests.post(token_url, data = params)
        data.raise_for_status()
        access_token = data.json()["access_token"]
        expires_in = data.json()["expires_in"]
        id_token = data.json()["id_token"]
        # track expiration status/time
        current_time = datetime.now()
        token_exp_time = current_time + timedelta(seconds=expires_in)
        api_call_headers = {'Authorization': 'Bearer ' + access_token}
        refresh_token = data.json()["refresh_token"]
        _log.info(f'Success - Token saved to api_call_headers variable and expires at {token_exp_time}')

    # handle errors
    except requests.exceptions.HTTPError as errh:
        _log.error(f"HTTP Error: {data.status_code} -- Double check your user name and password.")
    except requests.exceptions.ConnectionError as errc:
        _log.error(f"Connection Error: {data.status_code}")
    except requests.exceptions.Timeout as errt:
        _log.error(f"Timeout Error: {data.status_code}")
    except requests.exceptions.RequestException as err:
        _log.error(f"Request Error: {data.status_code}")

def refreshToken():
    """
    refreshToken() refreshes security tokens to extend access time for APIs
    that require authorization. It attempts to verify that a refresh token
    exists and recommends using getToken() to create a new token if needed.
    This function is called as needed by setApiUrl() and is generally not 
    something that needs to be called directly in your code.
    """
    global token_exp_time, api_call_headers, access_token, refresh_token, id_token

    try:
        token = refresh_token
    except NameError:
        _log.error("No token found. Create one using getToken().")
        raise StopExecution

    # refresh token request
    try:
        params = {'client_id': 'nbia',
                  'grant_type': 'refresh_token',
                  'refresh_token': token
                  }
        
        # obtain new access token
        data = requests.post(token_url, data = params)
        data.raise_for_status()
        access_token = data.json()["access_token"]
        expires_in = data.json()["expires_in"]

        # track expiration status/time 
        current_time = datetime.now()
        token_exp_time = current_time + timedelta(seconds=expires_in)
        api_call_headers = {'Authorization': 'Bearer ' + access_token}
        _log.info(f'Success - Token refreshed to api_call_headers variable and expires at {token_exp_time}')

    # handle errors
    except requests.exceptions.HTTPError as errh:
        _log.error(f"HTTP Error: {data.status_code} -- Token refresh failed. Create a new one with getToken().")
    except requests.exceptions.ConnectionError as errc:
        _log.error(f"Connection Error: {data.status_code}")
    except requests.exceptions.Timeout as errt:
        _log.error(f"Timeout Error: {data.status_code}")
    except requests.exceptions.RequestException as err:
        log.error(f"Request Error: {data.status_code}")

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


def queryData(endpoint, options, api_url, format):
    """
    queryData() is called by many other query functions and is generally
    not something that needs to be called directly in your code.
    It provides uses setApiURL() to set a base URL and addresses error
    handling for HTTP status and empty search results.
    Formats output as JSON by default with options for "df" (dataframe) and "csv"
    """
    # get base URL
    base_url = setApiUrl(endpoint, api_url)
    # display full URL with endpoint & parameters
    url = base_url + endpoint
    _log.info(f'Calling... {url} with parameters {options}')
    # get the data
    try:
        # include api_call_headers for restricted queries
        if api_url == "restricted" or endpoint in advancedEndpoints:
            data = requests.get(url, params = options, headers = api_call_headers)
        else:
            data = requests.get(url, params = options)
        data.raise_for_status()

        # check for empty results and format output
        if data.text != "":
            data = data.json()
            # format the output (optional)
            if format == "df":
                df = pd.DataFrame(data)
                return df
            elif format == "csv":
                df = pd.DataFrame(data)
                df.to_csv(endpoint + ".csv")
                _log.info(f"CSV saved to: {endpoint}.csv")
                return df
            else:
                return data
        else:
            _log.info("No results found.")

    # handle errors
    except requests.exceptions.HTTPError as errh:
        _log.error(errh)
    except requests.exceptions.ConnectionError as errc:
        _log.error(errc)
    except requests.exceptions.Timeout as errt:
        _log.error(errt)
    except requests.exceptions.RequestException as err:
        _log.error(err)


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
    NOTE: Unlike other API endpoints, this one expects MM/DD/YYYY, 
      but we convert from YYYY/MM/DD so tcia-utils date inputs are consistent.
    """
    endpoint = "getUpdatedSeries"

    # convert to NBIA's expected date format
    nbiaDate = datetime.strptime(date, "%Y/%m/%d").strftime("%m/%d/%Y")

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

def downloadSeries(series_data,
                   number = 0,
                   path = "",
                   hash = "",
                   api_url = "",
                   input_type = "",
                   format = "",
                   csv_filename = ""):
    """
    Ingests a set of seriesUids and downloads them.
    By default, series_data expects JSON containing "SeriesInstanceUID" elements.
    Set number = n to download the first n series if you don't want the full dataset.
    Set hash = "y" if you'd like to retrieve MD5 hash values for each image.
    Saves to tciaDownload folder in current directory if no path is specified.
    Set input_type = "list" to pass a list of Series UIDs instead of JSON.
    Set input_type = "manifest" to pass the path of a *.TCIA manifest file as series_data.
    Format can be set to "df" or "csv" to return series metadata.
    Setting a csv_filename will create the csv even if format isn't specified.
    The metadata includes info about series that have previously been
    downloaded if they're part of series_data.
    """
    endpoint = "getImage"
    seriesUID = ''
    success = 0
    failed = 0
    previous = 0

    # Prep a dataframe for later
    if format == "df" or format == "csv" or csv_filename != "":
        manifestDF=pd.DataFrame()

    # get base URL
    base_url = setApiUrl(endpoint, api_url)

    # if input = manifest convert manifest to python list of uids
    if input_type == "manifest":
        series_data = manifestToList(series_data)

    # set sample size if you don't want to download the full set of results
    if number > 0:
        _log.info(f"Downloading {number} out of {len(series_data)} Series Instance UIDs (scans).")
    else:
        _log.info(f"Downloading {len(series_data)} Series Instance UIDs (scans).")

    # set option to include md5 hashes
    if hash == "y":
        downloadOptions = "getImageWithMD5Hash?SeriesInstanceUID="
    else:
        downloadOptions = "getImage?NewFileNames=Yes&SeriesInstanceUID="

    # get the data
    try:
        for x in series_data:
            # specify whether input data is json or list
            if input_type == "list" or input_type == "manifest":
                seriesUID = x
            else:
                seriesUID = x['SeriesInstanceUID']
            # set path for downloads and check for previously downloaded data
            if path != "":
                pathTmp = path + "/" + seriesUID
            else:
                pathTmp = "tciaDownload/" + seriesUID
            # set URLs
            data_url = base_url + downloadOptions + seriesUID
            metadata_url = base_url + "getSeriesMetaData?SeriesInstanceUID=" + seriesUID
            # check if data was previously downloaded
            if not os.path.isdir(pathTmp):
                _log.info(f"Downloading... {data_url}")
                # check if headers are necessary
                if api_url == "restricted":
                    data = requests.get(data_url, headers = api_call_headers)
                else:
                    data = requests.get(data_url)
                # if download was successful
                if data.status_code == 200:
                    # get metadata if desired
                    if format == "df" or format == "csv" or csv_filename != "":
                        # check if headers are necessary for metadata retrieval
                        if api_url == "restricted":
                            metadata = requests.get(metadata_url, headers = api_call_headers).json()
                        else:
                            metadata = requests.get(metadata_url).json()
                        # write the series metadata to a dataframe
                        manifestDF = pd.concat([manifestDF, pd.DataFrame(metadata)], ignore_index=True)
                    # unzip file
                    file = zipfile.ZipFile(io.BytesIO(data.content))
                    file.extractall(path = pathTmp)
                    # count successes and break if number parameter is met
                    success += 1;
                    if number > 0:
                        if success == number:
                            break
                else:
                    _log.error(f"Error: {data.status_code} Series failed: {seriesUID}")
                    failed += 1;
            # if data has already been downloaded, only write metadata to df
            else:
                # get metadata if desired
                if format == "df" or format == "csv" or csv_filename != "":
                    if api_url == "restricted":
                        metadata = requests.get(metadata_url, headers = api_call_headers).json()
                    else:
                        metadata = requests.get(metadata_url).json()
                    # write the series metadata to a dataframe
                    manifestDF = pd.concat([manifestDF, pd.DataFrame(metadata)], ignore_index=True)
                _log.warning(f"Series {seriesUID} already downloaded.")
                previous += 1;
        # summarize download results
        if number > 0:
            _log.info(
                f"Downloaded {success} out of {number} requested series from a total of {len(series_data)} Series Instance UIDs (scans).\n"
                f"{failed} failed to download.\n"
                f"{previous} previously downloaded."
            )
        else:
            _log.info(
                f"Downloaded {success} out of {len(series_data)} Series Instance UIDs (scans).\n"
                f"{failed} failed to download.\n"
                f"{previous} previously downloaded."
            )
        # return metadata dataframe and/or save to CSV file if requested
        if csv_filename != "":
            manifestDF.to_csv(csv_filename + '.csv')
            _log.info(f"Series metadata saved as {csv_filename}.csv")
            return manifestDF
        if format == "csv" and csv_filename == "":
            now = datetime.now()
            dt_string = now.strftime("%Y-%m-%d_%H%M")
            manifestDF.to_csv('downloadSeries_metadata_' + dt_string + '.csv')
            _log.info(f"Series metadata saved as downloadSeries_metadata_{dt_string}.csv")
            return manifestDF
        if format == "df":
            return manifestDF

    except requests.exceptions.HTTPError as errh:
        _log.error(errh)
    except requests.exceptions.ConnectionError as errc:
        _log.error(errc)
    except requests.exceptions.Timeout as errt:
        _log.error(errt)
    except requests.exceptions.RequestException as err:
        _log.error(err)


def downloadImage(seriesUID,
                  sopUID,
                  path = "",
                  api_url = ""):
    """Ingests a seriesUids and SopInstanceUid and downloads the image"""
    endpoint = "getSingleImage"
    success = 0
    failed = 0
    previous = 0

    # get base URL
    base_url = setApiUrl(endpoint, api_url)

    try:
        if path != "":
            pathTmp = path + "/" + seriesUID
        else:
            pathTmp = "tciaDownload/" + seriesUID
        file = sopUID + ".dcm"
        if not os.path.isfile(pathTmp + "/" + file):
            data_url = base_url + 'getSingleImage?SeriesInstanceUID=' + seriesUID + '&SOPInstanceUID=' + sopUID
            _log.info(f"Downloading... {data_url}")
            if api_url == "restricted":
                data = requests.get(data_url, headers = api_call_headers)
                if data.status_code == 200:
                    if not os.path.exists(pathTmp):
                        os.makedirs(pathTmp)
                    with open(pathTmp + "/" + file, 'wb') as f:
                        f.write(data.content)
                    _log.info(f"Saved to {pathTmp}/{file}")
                else:
                    _log.error(
                        f"Error: {data.status_code} -- double check your permissions and Series/SOP UIDs.\n"
                        f"Series UID: {seriesUID}\n"
                        f"SOP UID: {sopUID}"
                    )
            else:
                data = requests.get(data_url)
                if data.status_code == 200:
                    if not os.path.exists(pathTmp):
                        os.makedirs(pathTmp)
                    with open(pathTmp + "/" + file, 'wb') as f:
                        f.write(data.content)
                    _log.info(f"Saved to {pathTmp}/{file}")
                else:
                    _log.error(
                        f"Error: {data.status_code} -- double check your permissions and Series/SOP UIDs.\n"
                        f"Series UID: {seriesUID}\n"
                        f"SOP UID: {sopUID}"
                    )
        else:
            _log.warning(f"Image {sopUID} already downloaded to:\n{pathTmp}")

    except requests.exceptions.HTTPError as errh:
        _log.error(errh)
    except requests.exceptions.ConnectionError as errc:
        _log.error(errc)
    except requests.exceptions.Timeout as errt:
        _log.error(errt)
    except requests.exceptions.RequestException as err:
        _log.error(err)

##########################
##########################
# Advanced API Endpoints


def getCollectionDescriptions(api_url = "", format = ""):
    """
    Gets HTML-formatted descriptions of collections and their DOIs
    """
    endpoint = "getCollectionDescriptions"
    options = {}

    data = queryData(endpoint, options, api_url, format)
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


def getSeriesList(list, api_url = "", csv_filename = "", format = ""):
    """
    Get metadata for a list of series from Advanced API.
    Returns result as dataframe (default) or as CSV if format = 'csv'.
    Use csv_filename to set a custom filename. 
    """
    uids = ",".join(list)
    param = {'list': uids}
    endpoint = "getSeriesMetadata2"

    # set base_url
    base_url = setApiUrl(endpoint, api_url)

    # full url
    url = base_url + endpoint
    _log.info(f'Calling... {url}')

    # get data & handle any request.post() errors
    try:
        metadata = requests.post(url, headers = api_call_headers, data = param)
        metadata.raise_for_status()

        # check for empty results and format output
        if metadata.text != "":
            df = pd.read_csv(io.StringIO(metadata.text), sep=',')
            if format == "csv" and csv_filename != "":
                df.to_csv(csv_filename + '.csv')
                _log.info(f"Report saved as {csv_filename}.csv")
            elif format == "csv":
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'series_report_{timestamp}.csv'
                grouped.to_csv(filename, index=False)
                _log.info(f"Collection summary report saved as '{filename}'")
            else:
                return df
        else:
            _log.info("No results found.")

    except requests.exceptions.HTTPError as errh:
        _log.error(errh)
    except requests.exceptions.ConnectionError as errc:
        _log.error(errc)
    except requests.exceptions.Timeout as errt:
        _log.error(errt)
    except requests.exceptions.RequestException as err:
        _log.error(err)


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



####### getDoiMetadata function
# Gets a list of Collections or Series associated with a DOI
# Requires a DOI URL and specification of Collection or Series UID output
# Result includes whether the data are 3rd party analyses or not
# Formats output as JSON by default with options for "df" (dataframe) and "csv"

def getDoiMetadata(doi, output = "", api_url = "", format = ""):
    """
    Optional: output, api_url, format
    Gets a list of Collections if output = "", or Series UIDs if output = "series", associated with a DOI.
    The result includes whether the data are 3rd party analyses or not.
    """
    param = {'DOI': doi,
             'CollectionOrSeries': output}

    endpoint = "getCollectionOrSeriesForDOI"

    # set base_url
    base_url = setApiUrl(endpoint, api_url)

    # full url
    url = base_url + endpoint
    _log.info(f'Calling... {url}')

    # get data & handle any request.post() errors
    try:
        metadata = requests.post(url, headers = api_call_headers, data = param)
        metadata.raise_for_status()

        # check for empty results and format output
        if metadata.text != "[]":
            metadata = metadata.json()
            # format the output (optional)
            if format == "df":
                df = pd.DataFrame(metadata)
                return df
            elif format == "csv":
                df = pd.DataFrame(metadata)
                df.to_csv(endpoint + ".csv")
                _log.info(f"CSV saved to: {endpoint}.csv")
                return df
            else:
                return metadata
        else:
            _log.info("No results found.")

    except requests.exceptions.HTTPError as errh:
        _log.error(errh)
    except requests.exceptions.ConnectionError as errc:
        _log.error(errc)
    except requests.exceptions.Timeout as errt:
        _log.error(errt)
    except requests.exceptions.RequestException as err:
        _log.error(err)


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

    except requests.exceptions.HTTPError as errh:
        _log.error(errh)
    except requests.exceptions.ConnectionError as errc:
        _log.error(errc)
    except requests.exceptions.Timeout as errt:
        _log.error(errt)
    except requests.exceptions.RequestException as err:
        _log.error(err)

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
    
    series_data can be provided as JSON, df, TCIA manifest file or python list.
    input_type informs the function which of those types are being used.
    If input_type = "manifest" the series_data should be the path to the manifest file.
    """

    # if input_type is manifest convert it to a list
    if input_type == "manifest":
        series_data = manifestToList(series_data)

    # if input_type is a list or manifest, download relevant metadata
    if input_type == "list" or input_type == "manifest":
        df = getSeriesList(series_data, api_url = "")
        # Rename the headers
        if df is None or df.empty:
            raise StopExecution
        else:
            df = df.rename(columns={'Subject ID': 'PatientID',
                                    'Study UID': 'StudyInstanceUID',
                                    'Series ID': 'SeriesInstanceUID',
                                    'Number of images': 'ImageCount',
                                    'Collection Name': 'Collection',
                                    'File Size (Bytes)': 'FileSize',
                                    'Data Description URI': 'CollectionURI',
                                    'License Name': 'LicenseName',
                                    'Series Number': 'SeriesNumber',
                                    'License URL': 'LicenseURI'})

    elif input_type == "df":
        df = series_data
    else:
        # Create a DataFrame from the series_data
        df = pd.DataFrame(series_data)
        
    # Ensure the DataFrame contains the necessary columns even if they are missing
    required_columns = ['Collection', 'CollectionURI', 'Modality', 'LicenseName',
                        'Manufacturer', 'BodyPartExamined', 'PatientID', 'StudyInstanceUID',
                        'SeriesInstanceUID', 'Series Description', 'SeriesNumber',
                        'ProtocolName', 'SeriesDate', 'ImageCount', 'FileSize', 'TimeStamp',
                        'ManufacturerModelName', 'SoftwareVersions', 'LicenseURI']
    
    for col in required_columns:
            if col not in df.columns:
                df[col] = None
    
    return df

def reportCollectionSummary(series_data, input_type="", api_url = "", format=""):
    """
    Generate a collection-oriented summary report from series metadata created by the
    output of getSeries(), getSeriesList(), getSharedcart() or getUpdatedSeries().

    This function calculates various statistics based on the input series_data, including:
    - Collection: List of unique collections (1 per row)
    - DOIs: List of unique values by collection
    - Modalities: List of unique values by collection
    - Licenses: List of unique values by collection
    - Manufacturers: List of unique values in the collection
    - Body Parts: List of unique values by collection
    - Subjects: Number of subjects by collection
    - Studies: Number of studies by collection
    - Series: Number of series by collection
    - Images: Number of images by collection
    - Disk Space: Formatted as KB/MB/GB/TB/PB by collection
    - TimeStamp Min: Earliest TimeStamp date by collection
    - TimeStamp Max: Latest TimeStamp date by collection
    - UniqueTimestamps: List of dates on which new series were published by collection

    Parameters:
    series_data: The input data to be summarized (expects JSON by default).
    input_type: Can be set to 'df' for dataframe, 'list' for python list, or 'manifest'.
                If manifest is used, series_data should be the path to the TCIA manifest file.
    format (str): Output format (default is dataframe, 'csv' for CSV file, 'chart' for charts).
    """
    # format series_data into df depending on input_type
    df = formatSeriesInput(series_data, input_type, api_url)
    
    # Group by Collection and calculate aggregated statistics
    grouped = df.groupby('Collection').agg({
        'CollectionURI': 'unique',
        'Modality': 'unique',
        'LicenseName': 'unique',
        'Manufacturer': 'unique',
        'BodyPartExamined': 'unique',
        'PatientID': 'nunique',
        'StudyInstanceUID': 'nunique',
        'SeriesInstanceUID': 'nunique',
        'ImageCount': 'sum',
        'FileSize': 'sum',
        'TimeStamp': ['min', 'max']
    }).reset_index()

    # Flatten the multi-level TimeStamp column and rename columns
    grouped.columns = [' '.join(col).strip() for col in grouped.columns.values]
    grouped.rename(columns={'TimeStamp min': 'Min TimeStamp',
                            'TimeStamp max': 'Max TimeStamp',
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
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
        unique_dates_df = df.groupby('Collection')['TimeStamp'].apply(lambda x: x.dt.date.unique()).reset_index()

    except:
        # if timestamps weren't provided in series_data, condense None values to unique string
        unique_dates_df = df.groupby('Collection')['TimeStamp'].apply(lambda x: x.unique()).reset_index()

    # rename columns
    unique_dates_df.columns = ['Collection', 'UniqueTimeStamps']

    # Merge the unique_dates_df with the grouped DataFrame
    grouped = grouped.merge(unique_dates_df, on='Collection', how='left')
    
    # Convert aggregated lists to strings & insert 'Not Specified' for null values
    grouped['DOIs'] = grouped['CollectionURI unique'].apply(lambda x: ', '.join(['Not Specified' if pd.isnull(val) or val == '' else val for val in x]))
    grouped['Modalities'] = grouped['Modality unique'].apply(lambda x: ', '.join(['Not Specified' if pd.isnull(val) or val == '' else val for val in x]))
    grouped['Licenses'] = grouped['LicenseName unique'].apply(lambda x: ', '.join(['Not Specified' if pd.isnull(val) or val == '' else val for val in x]))
    grouped['Manufacturers'] = grouped['Manufacturer unique'].apply(lambda x: ', '.join(['Not Specified' if pd.isnull(val) or val == '' else val for val in x]))
    grouped['Body Parts'] = grouped['BodyPartExamined unique'].apply(lambda x: ', '.join(['Not Specified' if pd.isnull(val) or val == '' else val for val in x]))
    grouped['Min TimeStamp'] = grouped['Min TimeStamp'].apply(lambda x: 'Not Specified' if pd.isnull(x) or x == '' else x)
    grouped['Max TimeStamp'] = grouped['Max TimeStamp'].apply(lambda x: 'Not Specified' if pd.isnull(x) or x == '' else x)
    grouped['UniqueTimeStamps'] = grouped['UniqueTimeStamps'].apply(lambda x: ', '.join(['Not Specified' if pd.isnull(val) or val == '' else val.strftime('%Y-%m-%d') for val in x]))

    
    # Remove unnecessary columns
    grouped.drop(columns=['CollectionURI unique', 'Modality unique', 'LicenseName unique',
                        'Manufacturer unique', 'BodyPartExamined unique'], inplace=True)

    # Reorder the columns
    grouped = grouped[['Collection', 'DOIs', 'Licenses', 'Subjects', 'Studies', 'Series', 'Images', 'File Size', 'Disk Space',
            'Body Parts', 'Modalities',  'Manufacturers', 'Min TimeStamp', 'Max TimeStamp', 'UniqueTimeStamps']]
    
    # generate charts if requested
    if format == 'chart':        
        
        # define collections
        collections = grouped['Collection'].tolist()
    
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
                create_pie_chart(data, label, collections)
            else:
                _log.info("No data available for " + label)

    if format == 'csv':
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'collection_report_{timestamp}.csv'
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
    fig = px.pie(df, names='Labels', values='Values', title=f'{metric_name} Distribution Across Collections')
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(showlegend=True, legend_title_text='Collections', width=width, height=height)

    # Show the pie chart
    fig.show()


def format_disk_space(size_in_bytes):
    """
    Helper function for reportCollections() to format bytes to other units.
    """
    if size_in_bytes < 1024 ** 2:
        return f'{size_in_bytes / 1024:.2f} KB'
    elif size_in_bytes < 1024 ** 3:
        return f'{size_in_bytes / (1024 ** 2):.2f} MB'
    elif size_in_bytes < 1024 ** 4:
        return f'{size_in_bytes / (1024 ** 3):.2f} GB'
    elif size_in_bytes < 1024 ** 5:
        return f'{size_in_bytes / (1024 ** 4):.2f} TB'
    else:
        return f'{size_in_bytes / (1024 ** 5):.2f} PB'
        
        
def reportSeriesReleaseDate(series_data, chart_width = 1024, chart_height = 768):
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
        print(f"Report saved to {filename}.")
    else:
        _log.info(report)

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


def viewSeries(seriesUid = "", path = ""):
    """
    Visualizes a Series (scan) you've downloaded in the notebook
    Requires EITHER a seriesUid or path parameter
    Leave seriesUid empty if you want to provide a custom path.
    The function assumes "tciaDownload/<seriesUid>/" as path if
    seriesUid is provided since this is where downloadSeries() saves data.
    """
    # set path where downloadSeries() saves the data if seriesUid is provided
    if seriesUid != "":
        path = "tciaDownload/" + seriesUid

    # error message function for when series doesn't exist or is invalid data
    def seriesInvalid(uid):
        if seriesUid:
            link = f"https://nbia.cancerimagingarchive.net/viewer/?series={seriesUID}"
        else:
            link = "https://nbia.cancerimagingarchive.net/viewer/?series=YOUR_SERIES_UID"
        _log.error(
            f"Cannot find a valid DICOM series at: {path}\n"
            'Try running downloadSeries(seriesUid, input_type = "uid") to download it first.'
            # "If the data isn't restricted, you can alternatively view it in your browser (without downloading) using this link:\n"
            # f"{link}"
        )

    # Verify series exists before visualizing
    if os.path.isdir(path):
        # load scan to pydicom
        slices = [pydicom.dcmread(path + '/' + s) for s in
                  os.listdir(path) if s.endswith(".dcm")]

        slices.sort(key = lambda x: int(x.InstanceNumber))

        try:
            modality = slices[0].Modality
        except IndexError:
            seriesInvalid(seriesUid)
            raise StopExecution

        image = np.stack([s.pixel_array for s in slices])
        image = image.astype(np.int16)

        if modality == "CT":
            # Set outside-of-scan pixels to 0
            # The intercept is usually -1024, so air is approximately 0
            image[image == -2000] = 0

            # Convert to Hounsfield units (HU)
            intercept = slices[0].RescaleIntercept
            slope = slices[0].RescaleSlope

            if slope != 1:
                image = slope * image.astype(np.float64)
                image = image.astype(np.int16)

            image += np.int16(intercept)

        pixel_data = np.array(image, dtype=np.int16)

        # slide through dicom images using a slide bar
        def dicom_animation(x):
            plt.imshow(pixel_data[x], cmap = plt.cm.gray)
            plt.show()
        interact(dicom_animation, x=(0, len(pixel_data)-1))
    else:
        seriesInvalid(seriesUid)


def viewSeriesSEG(seriesPath = "", SEGPath = ""):
    """
    Visualizes a Series (scan) you've downloaded and
    adds an overlay from the SEG series.
    Requires a path parameter for the reference series.
    Requires the file path for the annotation series.
    Used by the viewSeriesAnnotation() function.
    Not recommended to be used as a standalone function.
    """
    import pydicom_seg 
    slices = [pydicom.dcmread(seriesPath + '/' + s) for s in os.listdir(seriesPath) if s.endswith(".dcm")]
    slices.sort(key = lambda x: int(x.InstanceNumber), reverse = True)

    try:
        modality = slices[0].Modality
    except IndexError:
        seriesInvalid(seriesUid)
        raise StopExecution

    image = np.stack([s.pixel_array for s in slices])
    image = image.astype(np.int16)

    if modality == "CT":
        # Set outside-of-scan pixels to 0
        # The intercept is usually -1024, so air is approximately 0
        image[image == -2000] = 0

        # Convert to Hounsfield units (HU)
        intercept = slices[0].RescaleIntercept
        slope = slices[0].RescaleSlope

        if slope != 1:
            image = slope * image.astype(np.float64)
            image = image.astype(np.int16)

        image += np.int16(intercept)

    pixel_data = np.array(image, dtype=np.int16)     
    SEG_data = pydicom.dcmread(SEGPath)
    try:
        reader = pydicom_seg.MultiClassReader()
        result = reader.read(SEG_data)
    except ValueError:
        reader = pydicom_seg.SegmentReader()
        result = reader.read(SEG_data)

    if slices[0].SeriesInstanceUID != result.referenced_series_uid:
        raise Exception("The selected reference series and the annotation series don't match!")

    colorPaleatte = ["blue", "orange", "green", "red", "cyan", "brown", "lime", "purple", "yellow", "pink", "olive"] 
    def seg_animation(suppress_warnings, x, **kwargs):
        plt.imshow(pixel_data[x], cmap = plt.cm.gray)
        if isinstance(reader, pydicom_seg.reader.MultiClassReader):
            if kwargs[list(kwargs)[0]] == True:
                mask_data = result.data
                try:
                    plt.imshow(mask_data[x], cmap = plt.cm.rainbow, alpha = 0.5*(mask_data[x] > 0), interpolation = None)
                except IndexError:
                    if suppress_warnings == False:
                        _log.error(f"Visualization for the segment failed, it does not have the same slide count as the reference series.\nPlease use a DICOM workstation such as 3D Slicer to view the full dataset.")
        else:
            for i in result.available_segments:
                if i == 10 and len(result.available_segments) > 10:
                    print(f"Previewing first 10 of {len(result.available_segments)} labels. Please use a DICOM workstation such as 3D Slicer to view the full dataset.")
                if kwargs[list(kwargs)[i-1]] == True:
                    mask_data = result.segment_data(i)
                    cmap = matplotlib.colors.ListedColormap(colorPaleatte[i])
                    try:
                        plt.imshow(mask_data[x], cmap = cmap, alpha = 0.5*(mask_data[x] > 0), interpolation = None)
                    except IndexError:
                        if suppress_warnings == False:
                            _log.error(f"Visualization for segment {list(kwargs.keys())[i-1]} failed, it does not have the same slide count as the reference series.\nPlease use a DICOM workstation such as 3D Slicer to view the full dataset.")
        plt.axis('scaled')
        plt.show()

    if isinstance(reader, pydicom_seg.reader.MultiClassReader):
        kwargs = {"Show Segments": True}
        interact(seg_animation, suppress_warnings = False, x=(0, len(pixel_data)-1), **kwargs)
    else:
        kwargs = {f"{i+1} - {v.SegmentDescription}":True for i, v in enumerate(SEG_data.SegmentSequence[:10])}
        interact(seg_animation, suppress_warnings = False, x=(0, len(pixel_data)-1), **kwargs)


def viewSeriesRT(seriesPath = "", RTPath = ""):
    """
    Visualizes a Series (scan) you've downloaded and
    adds an overlay from the RTSTRUCT series.
    Requires a path parameter for the reference series.
    Requires the file path for the annotation series.
    Currenly not able to visualize seed points.
    Used by the viewSeriesAnnotation() function.
    Not recommended to be used as a standalone function.
    """
    import rt_utils 
    rtstruct = rt_utils.RTStructBuilder.create_from(seriesPath, RTPath)
    roi_names = rtstruct.get_roi_names()

    slices = rtstruct.series_data
    try:
        modality = slices[0].Modality
    except IndexError:
        seriesInvalid(seriesUid)
        raise StopExecution

    image = np.stack([s.pixel_array for s in slices])
    image = image.astype(np.int16)

    if modality == "CT":
        # Set outside-of-scan pixels to 0
        # The intercept is usually -1024, so air is approximately 0
        image[image == -2000] = 0

        # Convert to Hounsfield units (HU)
        intercept = slices[0].RescaleIntercept
        slope = slices[0].RescaleSlope

        if slope != 1:
            image = slope * image.astype(np.float64)
            image = image.astype(np.int16)

        image += np.int16(intercept)

    pixel_data = np.array(image, dtype=np.int16)
    colorPaleatte = ["blue", "orange", "green", "red", "cyan", "brown", "lime", "purple", "yellow", "pink", "olive"] 
    def rt_animation(suppress_warnings, x, **kwargs):
        plt.imshow(pixel_data[x], cmap = plt.cm.gray, interpolation = None)
        for i in range(len(kwargs)):
            if i == 9 and len(roi_names) > 10:
                print(f"Previewing first 10 of {len(roi_names)} labels. Please use a DICOM workstation such as 3D Slicer to view the full dataset.")
            if kwargs[f"{i+1} - {roi_names[i]}"] == True:
                try:
                    mask_data = rtstruct.get_roi_mask_by_name(roi_names[i])
                    cmap = matplotlib.colors.ListedColormap(colorPaleatte[i])
                    try:
                        plt.imshow(mask_data[:, :, x], cmap = cmap, alpha = 0.5*(mask_data[:, :, x] > 0), interpolation = None)
                    except IndexError:
                        if suppress_warnings == False:
                            _log.error(f"Visualization for segment {roi_names[i]} failed, it does not have the same slide count as the reference series.\nPlease use a DICOM workstation such as 3D Slicer to view the full dataset.")
                except Exception as e:
                    try:
                        if e.code == -215:
                            error_message = f"\nThe segment '{roi_names[i]}' is too small to visualize."
                        else:
                            error_message = f"\nThe segment '{roi_names[i]}' is too small to visualize."
                        if suppress_warnings == False: _log.error(error_message)
                        pass
                    except:
                        if suppress_warnings == False: _log.error(f"\n{e}")
                        pass
        plt.axis('scaled')
        plt.show()

    kwargs = {f"{i+1} - {v}": True for i, v in enumerate(roi_names[:10])}
    interact(rt_animation, suppress_warnings = False, x = (0, len(pixel_data)-1), **kwargs)


def viewSeriesAnnotation(seriesUid = "", seriesPath = "", annotationUid = "", annotationPath = ""):
    """
    Visualizes a Series (scan) you've downloaded and
    adds an overlay from the annotation series (SEG or RTSTRUCT).
    Directs to the correct visualization function depending
    on the modality of the annotation series.
    Requires EITHER a seriesUid or path parameter for the reference series.
    Requires EITHER a annotationUid or path parameter for the segmentation series.
    Opens a file browser for users to choose folder/file if
    the required parameters are not specified.
    Leave seriesUid and/or annotationUid empty if
    you want to provide a custom path.
    The function assumes "tciaDownload/<UID>/" as path if seriesUid and/or
    annotationUid is provided since this is where downloadSeries() saves data.
    Note that non-axial images might not be correctly displayed.
    """
    import tkinter
    from tkinter import filedialog
    def seriesInvalid(uid, path):
        if uid:
            link = f"https://nbia.cancerimagingarchive.net/viewer/?series={uid}"
        else:
            link = "https://nbia.cancerimagingarchive.net/viewer/?series=YOUR_SERIES_UID"
        _log.error(
            f"Cannot find a valid DICOM series at: {path}\n"
            'Try running downloadSeries(seriesUid, input_type = "uid") to download it first.'
            # "If the data isn't restricted, you can alternatively view it in your browser (without downloading) using this link:\n"
            # f"{link}"
        )

    if seriesUid == "" and seriesPath == "":
        try:
            tkinter.Tk().withdraw()
            folder_path = filedialog.askdirectory()
            seriesPath = folder_path
        except Exception:
            _log.error(
                f"\nYou are executing the function with unspecified parameters in an unsupported enviroment,"
                "\nplease specify the reference series UID or the folder path instead."
            )
            return
    elif seriesUid != "":
        seriesPath = "tciaDownload/" + seriesUid

    if annotationUid == "" and annotationPath == "":
        try:
            tkinter.Tk().withdraw()
            file_path = filedialog.askopenfilename()
            annotationPath = file_path
        except Exception:
            _log.error(
                f"\nYou are executing the function with unspecified parameters in an unsupported envrioment,"
                "\nplease specify the annotation series UID or the folder path instead."
            )
            return
    elif annotationUid != "":
        annotationPath = "tciaDownload/" + annotationUid + "/1-1.dcm"

    if os.path.isdir(seriesPath) and os.path.isfile(annotationPath):
        annotationModality = pydicom.dcmread(annotationPath).Modality
        if annotationModality == "SEG":
            viewSeriesSEG(seriesPath, annotationPath)
        elif annotationModality == "RTSTRUCT":
            viewSeriesRT(seriesPath, annotationPath)
        else:
            print("Wrong modality for the segmentation series, please check your selection.")
    elif not os.path.isdir(seriesPath):
        seriesInvalid(seriesUid, seriesPath)
    else:
        seriesInvalid(annotationUid, annotationPath)