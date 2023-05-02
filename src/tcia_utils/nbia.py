####### setup
import logging
import requests
import pandas as pd
import getpass
import json
import zipfile
import io
import os
from datetime import datetime
from datetime import timedelta
from enum import Enum
import matplotlib.pyplot as plt
import pydicom
import numpy as np
from ipywidgets import interact

class StopExecution(Exception):
    def _render_traceback_(self):
        pass

token_exp_time = datetime.now()
nlst_token_exp_time = datetime.now()

_log = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s'
    , level=logging.INFO
)

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

NPEXSpecies = {
    "human": 337915000
    , "mouse": 447612001
    , "dog": 448771007
}

####### setApiUrl()
# Called by other functions to select base URL
# Checks for valid security tokens where needed

def setApiUrl(endpoint, api_url):

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
                if datetime.now() > token_exp_time:
                    _log.info("Accessing Advanced API anonymously. To access restricted data use nbia.getToken() with your credentials.")
                    getToken(user = "nbia_guest")
                base_url = "https://services.cancerimagingarchive.net/nbia-api/services/"
        elif api_url == "nlst":
            if endpoint in searchEndpoints:
                # Using "Search" API with NLST server (no login required): https://wiki.cancerimagingarchive.net/x/fILTB
                base_url = "https://nlst.cancerimagingarchive.net/nbia-api/services/v1/"
            if endpoint in advancedEndpoints:
                # Using "Advanced" API docs (login required): https://wiki.cancerimagingarchive.net/x/YoATBg
                # Checking to see if a valid NLST authentication token exists
                if datetime.now() > nlst_token_exp_time:
                    getToken(user = "nbia_guest", api_url = "nlst")
                base_url = "https://nlst.cancerimagingarchive.net/nbia-api/services/"
        elif api_url == "restricted":
            if endpoint in searchEndpoints:
                # Using "Search with Authentication" API (login required): https://wiki.cancerimagingarchive.net/x/X4ATBg
                # Checking to see if a valid authentication token exists
                if datetime.now() < token_exp_time:
                    base_url = "https://services.cancerimagingarchive.net/nbia-api/services/v2/"
                else:
                    _log.error("Your security token for accessing the Restricted API is expired or does not exist. Create one using getToken().")
                    raise StopExecution
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

####### getToken()
# Retrieves security token to access APIs that require authorization
# Provides interactive prompts for user/pw if they're not specified as parameters
# Use getToken() for querying restricted collections with "Search API"
# Use getToken(api_url = "nlst") for "Advanced API" queries of National Lung Screening Trial
# Sets expiration time for tokens (2 hours from creation)

def getToken(user = "", pw = "", api_url = ""):

    global token_exp_time, nlst_token_exp_time, api_call_headers, nlst_api_call_headers

    # token URLs
    token_url = "https://services.cancerimagingarchive.net/nbia-api/oauth/token?username="
    nlst_token_url = "https://nlst.cancerimagingarchive.net/nbia-api/oauth/token?username="

    # set user name and password
    if user == "":
        print("Enter User: ")
        userName = input()
    else:
        userName = user
    if pw == "" and user != "nbia_guest":
        passWord = getpass.getpass(prompt = 'Enter Password: ')
    else:
        passWord = pw

    # create API token
    if api_url == "nlst":
        # create nlst token
        url = nlst_token_url + userName + "&password=" + passWord + "&grant_type=password&client_id=nbiaRestAPIClient&client_secret=ItsBetweenUAndMe"
    else:
        # create regular token
        url = token_url + userName + "&password=" + passWord + "&grant_type=password&client_id=nbiaRestAPIClient&client_secret=ItsBetweenUAndMe"

    try:
        data = requests.get(url)
        data.raise_for_status()
        access_token = data.json()["access_token"]
        # track expiration status/time (2 hours from creation)
        current_time = datetime.now()
        if api_url == "nlst":
            nlst_token_exp_time = current_time + timedelta(hours=2)
            nlst_api_call_headers = {'Authorization': 'Bearer ' + access_token}
            _log.info(f'Success - Token saved to nlst_api_call_headers variable and expires at {nlst_token_exp_time}')
        else:
            token_exp_time = current_time + timedelta(hours=2)
            api_call_headers = {'Authorization': 'Bearer ' + access_token}
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

####### makeCredentialFile()
# Create a credential file to use with NBIA Data Retriever
# Provides interactive prompts for user/pw if they're not specified as parameters
# Documentation at https://wiki.cancerimagingarchive.net/x/2QKPBQ

def makeCredentialFile(user = "", pw = ""):

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

####### queryData()
# Called by query functions that use requests.get()
# Provides error handling for requests.get()
# Formats output as JSON by default with options for "df" (dataframe) and "csv"

def queryData(endpoint, options, api_url, format):

    # get base URL
    base_url = setApiUrl(endpoint, api_url)
    # display full URL with endpoint & parameters
    url = base_url + endpoint
    _log.info(f'Calling... {url} with parameters {options}')
    # get the data
    try:
        # include api_call_headers for restricted queries
        if api_url == "restricted" or (endpoint in advancedEndpoints and api_url == ""):
            data = requests.get(url, params = options, headers = api_call_headers)
        # include nlst_api_call_headers for nlst-advanced
        elif api_url == "nlst" and endpoint in advancedEndpoints:
            data = requests.get(url, params = options, headers = nlst_api_call_headers)
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

####### getCollections function
# Gets a list of collections from a specified api_url

def getCollections(api_url = "",
                   format = ""):

    endpoint = "getCollectionValues"
    options = {}

    data = queryData(endpoint, options, api_url, format)
    return data

####### getBodyPart function
# Gets Body Part Examined metadata from a specified api_url
# Allows filtering by collection and modality

def getBodyPart(collection = "",
                modality = "",
                api_url = "",
                format = ""):

    endpoint = "getBodyPartValues"

    # create options dict to construct URL
    options = {}

    if collection:
        options['Collection'] = collection
    if modality:
        options['Modality'] = modality

    data = queryData(endpoint, options, api_url, format)
    return data

####### getModality function
# Gets Modalities metadata from a specified api_url
# Allows filtering by collection and bodyPart

def getModality(collection = "",
                bodyPart = "",
                api_url = "",
                format = ""):

    endpoint = "getModalityValues"

    # create options dict to construct URL
    options = {}

    if collection:
        options['Collection'] = collection
    if bodyPart:
        options['BodyPartExamined'] = bodyPart

    data = queryData(endpoint, options, api_url, format)
    return data

####### getPatient function
# Gets Patient metadata from a specified api_url
# Allows filtering by collection

def getPatient(collection = "",
               api_url = "",
               format = ""):

    endpoint = "getPatient"

    # create options dict to construct URL
    options = {}

    if collection:
        options['Collection'] = collection

    data = queryData(endpoint, options, api_url, format)
    return data

####### getPatientByCollectionAndModality function
# Gets Patient IDs from a specified api_url
# Requires specifying collection and modality
# Returns a list of patient IDs

def getPatientByCollectionAndModality(collection,
                                      modality,
                                      api_url = "",
                                      format = ""):

    endpoint = "getPatientByCollectionAndModality"

    # create options dict to construct URL
    options = {}
    options['Collection'] = collection
    options['Modality'] = modality

    data = queryData(endpoint, options, api_url, format)
    return data

####### getNewPatientsInCollection function
# Gets "new" patient metadata from a specified api_url
# Requires specifying collection and date
# Date format is YYYY/MM/DD

def getNewPatientsInCollection(collection,
                               date,
                               api_url = "",
                               format = ""):

    endpoint = "NewPatientsInCollection"

    # create options dict to construct URL
    options = {}
    options['Collection'] = collection
    options['Date'] = date

    data = queryData(endpoint, options, api_url, format)
    return data

####### getStudy function
# Gets Study (visit/timepoint) metadata from a specified api_url
# Requires filtering by collection
# Optional filters for patientId and studyUid

def getStudy(collection,
             patientId = "",
             studyUid = "",
             api_url = "",
             format = ""):

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

####### getNewStudiesInPatient function
# Gets "new" patient metadata from a specified api_url
# Requires specifying collection, patient ID and date
# Date format is YYYY/MM/DD

def getNewStudiesInPatient(collection,
                           patientId,
                           date,
                           api_url = "",
                           format = ""):

    endpoint = "NewStudiesInPatientCollection"

    # create options dict to construct URL
    options = {}
    options['Collection'] = collection
    options['PatientID'] = patientId
    options['Date'] = date

    data = queryData(endpoint, options, api_url, format)
    return data

####### getSeries function
# Gets Series (scan) metadata from a specified api_url
# Allows filtering by collection, patient ID, study UID,
#   series UID, modality, body part, manufacturer & model

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

####### getUpdatedSeries function
# Gets "new" series metadata from a specified api_url
# Requires specifying date
# Date format is YYYY/MM/DD
# NOTE: NBIA API expects MM/DD/YYYY (unlike any other API endpoint!)
#        so we'll convert from YYYY/MM/DD so tcia-utils is consistent

def getUpdatedSeries(date,
                     api_url = "",
                     format = ""):

    endpoint = "getUpdatedSeries"

    # convert to NBIA's expected date format
    nbiaDate = datetime.strptime(date, "%Y/%m/%d").strftime("%m/%d/%Y")

    # create options dict to construct URL
    options = {}
    options['fromDate'] = nbiaDate

    data = queryData(endpoint, options, api_url, format)
    return data

####### getSeriesMetadata function
# Gets Series (scan) metadata from a specified api_url
# Requires a specific Series Instance UID as input
# Output includes DOI and license details that are not in getSeries()

def getSeriesMetadata(seriesUid,
                      api_url = "",
                      format = ""):

    endpoint = "getSeriesMetaData"

    # create options dict to construct URL
    options = {}
    options['SeriesInstanceUID'] = seriesUid

    data = queryData(endpoint, options, api_url, format)
    return data

####### getSeriesSize function
# Gets the file count and disk size of a series/scan
# Requires Series Instance UID as input

def getSeriesSize(seriesUid,
                  api_url = "",
                  format = ""):

    endpoint = "getSeriesSize"

    # create options dict to construct URL
    options = {}
    options['SeriesInstanceUID'] = seriesUid

    data = queryData(endpoint, options, api_url, format)
    return data

####### getSopInstanceUids function
# Gets SOP Instance UIDs from a specific series/scan
# Requires a specific Series Instance UID as input

def getSopInstanceUids(seriesUid,
                       api_url = "",
                       format = ""):

    endpoint = "getSOPInstanceUIDs"

    # create options dict to construct URL
    options = {}
    options['SeriesInstanceUID'] = seriesUid

    data = queryData(endpoint, options, api_url, format)
    return data

####### getManufacturer function
# Gets manufacturer metadata from a specified api_url
# Allows filtering by collection, body part & modality

def getManufacturer(collection = "",
                    modality = "",
                    bodyPart = "",
                    api_url = "",
                    format = ""):

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

####### getSharedCart function
# Gets "Shared Cart" (scan) metadata from a specified api_url
# Use https://nbia.cancerimagingarchive.net/nbia-search/ to create a cart
# Add data to your basket, then click "Share" > "Share my cart"
# The "name" parameter is part of the URL that generates.
# E.g https://nbia.cancerimagingarchive.net/nbia-search/?saved-cart=nbia-49121659384603347
#  has a cart "name" of "nbia-49121659384603347".

def getSharedCart(name,
                  api_url = "",
                  format = ""):

    endpoint = "getContentsByName"

    # create options dict to construct URL
    options = {}
    options['name'] = name

    data = queryData(endpoint, options, api_url, format)
    return data

####### downloadSeries function
# Ingests a set of seriesUids and downloads them
# By default, series_data expects JSON containing "SeriesInstanceUID" elements
# Set number = n to download the first n series if you don't want the full dataset
# Set hash = "y" if you'd like to retrieve MD5 hash values for each image
# Saves to tciaDownload folder in current directory if no path is specified
# Set input_type = "list" to pass a list of Series UIDs instead of JSON
# Set input_type = "manifest" to pass the path of a *.TCIA manifest file as series_data
# Format can be set to "df" or "csv" to return series metadata
# Setting a csv_filename will create the csv even if format isn't specified
# The metadata includes info about series that have previously been downloaded

def downloadSeries(series_data,
                   number = 0,
                   path = "",
                   hash = "",
                   api_url = "",
                   input_type = "",
                   format = "",
                   csv_filename = ""):

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

####### downloadImage function
# Ingests a seriesUids and SopInstanceUid and downloads the image

def downloadImage(seriesUID,
                  sopUID,
                  path = "",
                  api_url = ""):

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

####### getCollectionDescriptions function (Advanced)
# Get HTML-formatted descriptions of collections and their DOIs

def getCollectionDescriptions(api_url = "", format = ""):
    endpoint = "getCollectionDescriptions"
    options = {}

    data = queryData(endpoint, options, api_url, format)
    return data

####### getCollectionPatientCounts function (Advanced)
# Get patient counts by collection from Advanced API

def getCollectionPatientCounts(api_url = "", format = ""):

    endpoint = "getCollectionValuesAndCounts"
    options = {}

    data = queryData(endpoint, options, api_url, format)
    return data

####### getModalityCounts function (Advanced)
# Get counts of Modality metadata from Advanced API
# Allows filtering by collection and bodyPart

def getModalityCounts(collection = "",
                      bodyPart = "",
                      api_url = "",
                      format = ""):

    endpoint = "getModalityValuesAndCounts"

    # create options dict to construct URL
    options = {}

    if collection:
        options['Collection'] = collection
    if bodyPart:
        options['BodyPartExamined'] = bodyPart

    data = queryData(endpoint, options, api_url, format)
    return data

####### getBodyPartCounts function (Advanced)
# Get counts of Body Part metadata from Advanced API
# Allows filtering by collection and modality

def getBodyPartCounts(collection = "",
                      modality = "",
                      api_url = "",
                      format = ""):

    endpoint = "getBodyPartValuesAndCounts"

    # create options dict to construct URL
    options = {}

    if collection:
        options['Collection'] = collection
    if modality:
        options['Modality'] = modality

    data = queryData(endpoint, options, api_url, format)
    return data

####### getManufacturerCounts function (Advanced)
# Get counts of Manufacturer metadata from Advanced API
# Allows filtering by collection, body part and modality

def getManufacturerCounts(collection = "",
                      modality = "",
                      bodyPart = "",
                      api_url = "",
                      format = ""):

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

####### getSeriesList function (Advanced)
# Get series metadata from Advanced API
# Allows submission of a list of UIDs
# Returns result as dataframe and CSV

def getSeriesList(list, api_url = "", csv_filename = ""):

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
        if api_url == "nlst":
            metadata = requests.post(url, headers = nlst_api_call_headers, data = param)
        else:
            metadata = requests.post(url, headers = api_call_headers, data = param)
        metadata.raise_for_status()

        # check for empty results and format output
        if metadata.text != "":
            df = pd.read_csv(io.StringIO(metadata.text), sep=',')
            if csv_filename != "":
                df.to_csv(csv_filename + '.csv')
                _log.info(f"Report saved as {csv_filename}.csv")
            else:
                df.to_csv('scan_metadata.csv')
                _log.info("Report saved as scan_metadata.csv")
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

####### getDicomTags function (Advanced)
# Gets DICOM tag metadata for a given series UID (scan)

def getDicomTags(seriesUid,
                 api_url = "",
                 format = ""):

    endpoint = "getDicomTags"

    # create options dict to construct URL
    options = {}
    options['SeriesUID'] = seriesUid

    data = queryData(endpoint, options, api_url, format)
    return data

####### getDoiMetadata function
# Gets a list of Collections or Series associated with a DOI
# Requires a DOI URL and specification of Collection or Series UID output
# Result includes whether the data are 3rd party analyses or not
# Formats output as JSON by default with options for "df" (dataframe) and "csv"

def getDoiMetadata(doi, output, api_url = "", format = ""):

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
        if api_url == "nlst":
            metadata = requests.post(url, headers = nlst_api_call_headers, data = param)
        else:
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

####### getSimpleSearchWithModalityAndBodyPartPaged function
# Takes the same parameters as the SimpleSearch GUI
# Using more parameters narrows the number of subjects received.
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
    Takes the same parameters as the SimpleSearch GUI; using more parameters narrows
    the number of subjects received.

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
        if api_url == "nlst":
            metadata = requests.post(url, headers = nlst_api_call_headers, data = options)
        else:
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
# Miscellaneous

def makeSeriesReport(series_data, input_type = "", format = "", filename = None, api_url = ""):
# Ingests JSON output from any function that returns series-level data and creates summary report
# Specify input_type = "manifest" to ingest a *.TCIA manifest file or "list" for a python list of UIDs
# If input_type = "manifest" or "list" and there are series UIDs that are restricted
#    you must call getToken() with a user ID that has access to all UIDs before calling this function.
# Specifying api_url is only necessary if you are using input_type = "manifest" or "list" with NLST data (e.g. api_url = "nlst") 
# Specify format = "var" to return the report values as a dictionary
# Access variables example after saving function output to report_data: subjects = report_data["subjects"]
# Specify format = "file" to save the report to a file
# Specify a filename parameter to set a filename if you don't want the default


    # if input_type is manifest convert it to a list
    if input_type == "manifest":
        series_data = manifestToList(series_data)
        
    # if input_type is a list or manifest download relevant metadata
    if input_type == "list" or input_type == "manifest":
        df = getSeriesList(series_data, api_url = "", csv_filename = "")
        # Rename the headers
        if df is None or df.empty:
            raise StopExecution
        else:
            df = df.rename(columns={'Subject ID': 'PatientID', 'Study UID': 'StudyInstanceUID', 'Series ID': 'SeriesInstanceUID', 'Number of images': 'ImageCount', 'Collection Name': 'Collection'})
            # Add an empty column called "BodyPartExamined" since getSeriesList() doesn't return this info -- FEATURE REQUEST SUBMITTED TO ADD THIS
            df['BodyPartExamined'] = ''
    else:
        # Create a DataFrame from the series_data
        df = pd.DataFrame(series_data)

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

####### manifestToList function
# Ingests a TCIA manifest file and removes header
# Returns a list of series UIDs

def manifestToList(manifest):

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

####### makeVizLinks function
# Ingests JSON output of getSeries() or getSharedCart()
# Creates URLs to visualize them in a browser
# The links appear in the last 2 columns of the dataframe
# TCIA links display the individual series described in each row
# IDC links display the entire study (all scans from that time point)
# IDC links may not work if they haven't mirrored the series from TCIA yet
# This function only works with fully public datasets (no limited-access data)
# Optionally accepts a csv_filename parameter if you'd like to export a CSV file

def makeVizLinks(series_data, csv_filename=""):

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

####### viewSeries function
# Visualize a Series (scan) you've downloaded in the notebook
# Requires EITHER a seriesUid or path parameter
# Leave seriesUid empty if you want to provide a custom path
# The function assumes "tciaDownload/<seriesUid>/" as path if seriesUid is
#   provided since this is where downloadSeries() saves data

def viewSeries(seriesUid = "", path = ""):

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
            'Try running downloadSeries(seriesUid, input_type = "uid") to download it first.\n'
            "If the data isn't restricted, you can alternatively view it in your browser (without downloading) using this link:\n"
            f"{link}"
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
