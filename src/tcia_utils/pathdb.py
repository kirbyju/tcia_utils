import pandas as pd
import requests
from datetime import datetime
import plotly.graph_objects as go
import numpy as np
import logging
from tcia_utils.utils import searchDf
from tcia_utils.utils import copy_df_cols
import os
from urllib.parse import urlparse
import concurrent.futures
from tqdm import tqdm

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
        extracted_item['slideId'] = item.get('nid')[0].get('value')
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
        fig = go.Figure(data=[
            go.Bar(name='Subject Count', x=summary['collectionName'], y=summary['subjectCount'], marker_color='blue'),
            go.Bar(name='Image Count', x=summary['collectionName'], y=summary['imageCount'], marker_color='green')
        ])

        fig.update_layout(
            title='Subject and Image Counts by Collection',
            xaxis_title='Collection Name',
            yaxis_title='Counts',
            barmode='group'
        )

        fig.show()

    elif format == 'csv':
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'pathdb_report_{timestamp}.csv'
        summary.to_csv(filename, index=False)
        print(f"Summary report saved as '{filename}'")

    return summary


def _download_image(image_info, path):
    """Helper function to download a single image."""
    try:
        image_url = image_info.get("imageUrl")
        if not image_url:
            return None, image_info.get('imageId', 'Unknown')

        parsed_url = urlparse(image_url)
        filename = os.path.basename(parsed_url.path)
        if not filename:
            filename = f"{image_info.get('imageId', 'unnamed_image')}.jpg"

        filepath = os.path.join(path, filename)

        response = requests.get(image_url, stream=True, allow_redirects=True)
        response.raise_for_status()

        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return filename, None
    except Exception as e:
        _log.error(f"An error occurred while processing {image_info.get('imageUrl')}: {e}")
        return None, image_info.get('imageId', 'Unknown')


def downloadImages(images_data, path="pathdb_images", max_workers=10, number: int = 0):
    """
    Downloads images from a list of image data objects.

    This function takes the output from getImages() and downloads the files
    concurrently. It checks for existing files to avoid re-downloading.

    Args:
        images_data (list or pd.DataFrame): A list of dictionaries or a pandas DataFrame
            containing image metadata, including an 'imageUrl' key.
        path (str): The local directory to save the downloaded images.
        max_workers (int): The maximum number of concurrent download threads.
        number (int): The maximum number of new images to download. Defaults to 0 (no limit).
    """
    if isinstance(images_data, pd.DataFrame):
        images_data = images_data.to_dict('records')

    if not images_data:
        _log.info("No images to download.")
        return

    try:
        if not os.path.exists(path):
            os.makedirs(path)
            _log.info(f"Directory '{path}' created successfully.")
        else:
            _log.info(f"Directory '{path}' already exists.")
    except OSError as e:
        _log.error(f"Failed to create directory '{path}': {e}")
        return

    # Identify new images to download
    existing_files = set(os.listdir(path))
    images_to_download = []
    previously_downloaded_count = 0
    for image_info in images_data:
        image_url = image_info.get("imageUrl")
        if not image_url:
            continue
            
        parsed_url = urlparse(image_url)
        filename = os.path.basename(parsed_url.path)
        if not filename:
            filename = f"{image_info.get('imageId', 'unnamed_image')}.jpg"

        if filename in existing_files:
            previously_downloaded_count += 1
        else:
            images_to_download.append(image_info)
            
    if number > 0:
        images_to_download = images_to_download[:number]

    _log.info(f"Found {previously_downloaded_count} previously downloaded images.")
    _log.info(f"Attempting to download {len(images_to_download)} new images.")

    # Download files concurrently
    success_count = 0
    failed_count = 0
    if images_to_download:
        with tqdm(total=len(images_to_download), desc="Downloading images", unit="file") as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_image = {executor.submit(_download_image, image_info, path): image_info for image_info in images_to_download}
                
                for future in concurrent.futures.as_completed(future_to_image):
                    filename, error_id = future.result()
                    if filename:
                        success_count += 1
                    else:
                        failed_count += 1
                    pbar.update(1)

    _log.info(
        f"Download complete. "
        f"Successfully downloaded: {success_count}. "
        f"Failed: {failed_count}. "
        f"Previously downloaded: {previously_downloaded_count}."
    )


def reportSubmissions(df, useCreated=False, format=None):
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
        fig = go.Figure(data=[
            go.Bar(name='Collection Count', x=summary['year'], y=summary['collectionCount'], marker_color='blue'),
            go.Bar(name='Subject Count', x=summary['year'], y=summary['subjectCount'], marker_color='orange'),
            go.Bar(name='Image Count', x=summary['year'], y=summary['imageCount'], marker_color='green')
        ])

        fig.update_layout(
            title='Submissions Counts by Year',
            xaxis_title='Year',
            yaxis_title='Counts',
            barmode='group'
        )

        fig.show()

    elif format == 'csv':
        time_type = 'created' if useCreated else 'changed'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'submissions_report_{time_type}_{timestamp}.csv'
        summary.to_csv(filename, index=False)
        print(f"Submissions report saved as '{filename}'")

    return summary
