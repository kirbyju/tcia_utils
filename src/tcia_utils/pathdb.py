import pandas as pd
import requests
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import logging
from tcia_utils.utils import searchDf
from tcia_utils.utils import copy_df_cols

_log = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s'
    , level=logging.INFO
)

base_url = 'https://pathdb.cancerimagingarchive.net/'


def getCollections(query = "", format = ""):
    """
    Use "query" parameter to search collection names.
    Format parameter can be set to "df" for dataframe
    or "csv" to save it to a file.
    """

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

def getImages(query, format=""):
    """
    Use "query" parameter to search collection names or
    enter a specific collection ID.
    Function returns JSON, but format parameter can be set
    to "df" for dataframe or "csv" to save it to a file.
    """
    base_url = 'https://pathdb.cancerimagingarchive.net/'
    extracted_data = []

    def extractFields(item):
        extracted_item = {}
        extracted_item['collectionName'] = item.get('studyid')[0].get('value')
        extracted_item['collectionId'] = item.get('field_collection')[0].get('target_id')
        extracted_item['subjectId'] = item.get('clinicaltrialsubjectid')[0].get('value')
        extracted_item['imageId'] = item.get('imageid')[0].get('value')
        extracted_item['imageHeight'] = item.get('imagedvolumeheight')[0].get('value')
        extracted_item['imagedWidth'] = item.get('imagedvolumewidth')[0].get('value')

        reference_pixel_x = item.get('referencepixelphysicalvaluex', [{}])
        physical_pixel_x = reference_pixel_x[0].get('value') if reference_pixel_x else None
        extracted_item['physicalPixelSizeX'] = physical_pixel_x if physical_pixel_x is not None else ""

        reference_pixel_y = item.get('referencepixelphysicalvaluey', [{}])
        physical_pixel_y = reference_pixel_y[0].get('value') if reference_pixel_y else None
        extracted_item['physicalPixelSizeY'] = physical_pixel_y if physical_pixel_y is not None else ""

        image_field = item.get('field_wsiimage', [{}])
        extracted_item['imageUrl'] = image_field[0].get('url', "")

        created_field = item.get('created', [{}])
        created_value = created_field[0].get('value', "")
        extracted_item['created'] = datetime.strptime(created_value, "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d %H-%M-%S")

        changed_field = item.get('changed', [{}])
        changed_value = changed_field[0].get('value', "")
        extracted_item['changed'] = datetime.strptime(changed_value, "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d %H-%M-%S")

        return extracted_item

    def getResults(url):
        page = 0

        while True:
            paginated_url = f"{url}&page={page}" if '?' in url else f"{url}?page={page}"
            _log.info(f'Calling... {paginated_url}')
            response = requests.get(paginated_url)
            
            if response.status_code == 200:
                data = response.json()
                if len(data) == 0:
                    break  # No more pages, exit the loop

                # Extract desired fields from the JSON data
                for item in data:
                    extracted_item = extractFields(item)
                    extracted_data.append(extracted_item)
            else:
                _log.error(f"Error: {response.status_code} - {response.reason}")
                return None

            page += 1

    ### Running this query against all collections is not currently feasible
    ### due to performance issues. Leaving this as a placeholder in case 
    ### it becomes feasible later.
    
    #if query is None:
    #    url = base_url + 'listofimages?_format=json'
    #    getResults(url)
    
    # if query was a collection ID (integer)
    if isinstance(query, int):
        url = base_url + 'listofimages/' + str(query) + '?_format=json'
        getResults(url)
    # if query is a string, look for matching collection names
    else:
        collections = getCollections(query=query)
        # iterate through all collections that matched query
        for x in collections:
            id = x["collectionId"]
            url = base_url + 'listofimages/' + str(id) + '?_format=json'
            getResults(url)

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
        
        
def reportCollections(df, yearCreated=None, yearChanged=None, format=None):
    """
    Generate a Collection summary report from the result of getImages(query, format = "df).

    This function calculates the number of subjects and images for each collection.
    It returns the result as a dataframe if no format is specified.
    It can optionally format the summary as a bar chart or save it as a CSV file.

    Parameters:
    df (DataFrame): The input DataFrame containing collection data.
    yearCreated (int): Filter the data by the year created (optional).
    yearChanged (int): Filter the data by the year changed (optional).
    format (str): Output format ('chart' for bar chart, 'csv' for CSV file, default is None).
    """
    if yearCreated:
        df['yearCreated'] = pd.to_datetime(df['created']).dt.year
        df = df[df['yearCreated'] == yearCreated]
    if yearChanged:
        df['yearChanged'] = pd.to_datetime(df['changed']).dt.year
        df = df[df['yearChanged'] == yearChanged]
    
    summary = df.groupby('collectionName').agg(
        subjectCount=pd.NamedAgg(column='subjectId', aggfunc='nunique'),
        imageCount=pd.NamedAgg(column='imageId', aggfunc='nunique'),
        lastCreated=pd.NamedAgg(column='created', aggfunc='max'),
        lastChanged=pd.NamedAgg(column='changed', aggfunc='max')
    ).reset_index()
    
    if format == 'chart':
        plt.figure(figsize=(10, 6))

        bar_width = 0.35
        index = np.arange(len(summary['collectionName']))

        plt.bar(index, summary['subjectCount'], bar_width, label='Subject Count', color='blue')
        plt.bar(index + bar_width, summary['imageCount'], bar_width, label='Image Count', color='green')

        plt.xlabel('Collection Name')
        plt.ylabel('Counts')
        plt.title('Subject and Image Counts by Collection')
        plt.xticks(index + bar_width / 2, summary['collectionName'], rotation=45, ha='right')
        plt.legend()

        plt.tight_layout()
        plt.show()
        
    elif format == 'csv':
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'pathdb_report_{timestamp}.csv'
        summary.to_csv(filename, index=False)
        print(f"Summary report saved as '{filename}'")
        
    return summary
    

def reportSubmissions(df, useCreated=False, format=None):
    """
    Generate a Submissions summary report from a DataFrame containing submission data.

    This function calculates the number of collections, subjects, and images for each year.
    It returns the result as a DataFrame if no format is specified.
    It can optionally format the summary as a bar chart or save it as a CSV file.

    Parameters:
    df (DataFrame): The input DataFrame containing submission data.
    useCreated (bool): If True, use 'created' column for calculations; otherwise, use 'changed' (default).
    format (str): Output format ('chart' for bar chart, 'csv' for CSV file, default is None).
    """
    if useCreated:
        time_column = 'created'
    else:
        time_column = 'changed'
    
    df[time_column] = pd.to_datetime(df[time_column])
    df['year'] = df[time_column].dt.year
    
    summary = df.groupby('year').agg(
        collectionCount=pd.NamedAgg(column='collectionName', aggfunc='nunique'),
        subjectCount=pd.NamedAgg(column='subjectId', aggfunc='nunique'),
        imageCount=pd.NamedAgg(column='imageId', aggfunc='nunique')
    ).reset_index()
    
    if format == 'chart':
        plt.figure(figsize=(10, 6))

        bar_width = 0.25
        index = np.arange(len(summary['year']))

        plt.bar(index, summary['collectionCount'], bar_width, label='Collection Count', color='blue')
        plt.bar(index + bar_width, summary['subjectCount'], bar_width, label='Subject Count', color='orange')
        plt.bar(index + 2 * bar_width, summary['imageCount'], bar_width, label='Image Count', color='green')

        plt.xlabel('Year')
        plt.ylabel('Counts')
        plt.title('Submissions Counts by Year')
        plt.xticks(index + bar_width, summary['year'], rotation=45, ha='right')
        plt.legend()

        plt.tight_layout()
        plt.show()
        
    elif format == 'csv':
        time_type = 'created' if useCreated else 'changed'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'submissions_report_{time_type}_{timestamp}.csv'
        summary.to_csv(filename, index=False)
        print(f"Submissions report saved as '{filename}'")
        
    return summary