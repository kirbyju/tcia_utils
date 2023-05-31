import pandas as pd
import requests
from datetime import datetime
import logging

_log = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s'
    , level=logging.INFO
)

base_url = 'https://pathdb.cancerimagingarchive.net/'

###################
# getCollections()
# use "query" parameter to search collection names
# format parameter can be set to "df" for dataframe
#    or "csv" to save it to a file

def getCollections(query = "", format = ""):

    extracted_data = []
    url = base_url + 'collections?_format=json'
    _log.info(f'Calling... {url}')
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()

        # Extract desired fields from the JSON data
        for item in data:
            extracted_item = {}
            extracted_item['collectionName'] = item.get('name')[0].get('value')
            extracted_item['collectionId'] = item.get('tid')[0].get('value')
            # extract and format date collection was updated
            tmp_date = item.get('changed')[0].get('value')
            parsed_datetime = datetime.fromisoformat(tmp_date)
            formatted_date = parsed_datetime.strftime("%Y-%m-%d")
            extracted_item['updated'] = formatted_date
            extracted_data.append(extracted_item)
    else:
        _log.error(f"Error: {response.status_code} - {response.reason}")
        return None

    # Filter extracted data based on query parameter
    if query:
        extracted_data = [item for item in extracted_data if query.lower() in item['collectionName'].lower()]

    # Convert the extracted data to a DataFrame
    if format == "df":
        df = pd.DataFrame(extracted_data)
        return df
    elif format == "csv":
        df = pd.DataFrame(extracted_data)
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"pathologyCollections-{today}.csv"
        df.to_csv(filename, index=False)
        _log.info(f"File saved to {filename}.")
    else:
        return extracted_data
        
###################
# getImages()
# use "query" parameter to search collection names or enter a specific collection ID
# returns JSON, but format parameter can be set to "df" for dataframe
#    or "csv" to save it to a file

def getImages(query, format=""):
    
    collectionList = []  # for queries that match multiple collection names
    extracted_data = []

    def getPaginatedResults(id):
        page = 0  

        while True:
            url = base_url + 'listofimages/' + str(id) + '?page=' + str(page) + '&_format=json'
            _log.info(f'Calling... {url}')
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if len(data) == 0:
                    break  # No more pages, exit the loop
                
                # Extract desired fields from the JSON data
                for item in data:
                    extracted_item = {}
                    extracted_item['collectionName'] = item.get('studyid')[0].get('value')
                    extracted_item['collectionId'] = item.get('field_collection')[0].get('target_id')
                    extracted_item['subjectId'] = item.get('clinicaltrialsubjectid')[0].get('value')
                    extracted_item['imageId'] = item.get('imageid')[0].get('value')
                    extracted_item['imageHeight'] = item.get('imagedvolumeheight')[0].get('value')
                    extracted_item['imagedWidth'] = item.get('imagedvolumewidth')[0].get('value')
                    extracted_item['physicalPixelSizeX'] = item.get('referencepixelphysicalvaluex')[0].get('value')
                    extracted_item['physicalPixelSizeY'] = item.get('referencepixelphysicalvaluey')[0].get('value')
                    extracted_item['imageUrl'] = item.get('field_wsiimage')[0].get('url')
                    extracted_data.append(extracted_item)
            else:
                _log.error(f"Error: {response.status_code} - {response.reason}")
                return None
            page += 1
        return extracted_data
    
    # if query was a collection ID (integer)
    if isinstance(query,int):
        extracted_data = getPaginatedResults(query)
    # if query is a string, look for matching collection names
    else:  
        collections = getCollections(query=query)
        # iterate through all collections that matched query
        for x in collections:
            id = x["collectionId"]
            extracted_data = getPaginatedResults(id)

    # Convert the extracted data to a DataFrame
    if format == "df":
        df = pd.DataFrame(extracted_data)
        return df
    elif format == "csv":
        df = pd.DataFrame(extracted_data)
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"pathologyImages-{today}.csv"
        df.to_csv(filename, index=False)
    else:
        return extracted_data