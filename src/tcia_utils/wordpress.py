import pandas as pd
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging
from tcia_utils.utils import searchDf
from tcia_utils.utils import remove_html_tags
from tcia_utils.utils import copy_df_cols

_log = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s'
    , level=logging.INFO
)

base_url = "https://cancerimagingarchive.net/api/v1/"

def getQuery(endpoint, per_page, format="", file_name=None, fields=None, ids=None, query=None, removeHtml=None):
    """
    Handle query basics that are common to all endpoints such as
    paging results, setting output formats, and file names.

    Args:
        endpoint (str): The API endpoint to query.
        per_page (int, optional): Number of results per page (default is 250).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).

    Returns:
        list or DataFrame: Retrieved results based on the specified parameters and format.

    Raises:
        ValueError: If an invalid format is provided.

    """
    # Append custom fields to the endpoint if provided
    if fields:
        fields_str = ','.join(fields)
        endpoint += f"?_fields={fields_str}"
    
    # Set URL 
    url = base_url + endpoint
    
    # Set the request parameters
    params = {'per_page': per_page}
    
    # Add ids to the parameters if provided
    if ids:
        ids_str = ','.join(str(id) for id in ids)
        params['include'] = ids_str
    
    # Add query to the parameters if provided
    if query:
        params['search'] = query
    
    # Make a GET request to the API endpoint with the parameters
    _log.info('Requesting %s', url)
    response = requests.get(url, params=params)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Check if there are more pages to fetch
        while 'next' in response.links.keys():
            next_url = response.links['next']['url']
            _log.info('Requesting %s', next_url)
            response = requests.get(next_url)
            if response.status_code == 200:
                data.extend(response.json())
            else:
                _log.error('Error accessing the API: %s', response.status_code)
                break
        
        # Save or return the output based on the format
        if format == "json" or format == "":
            # Save as JSON
            if file_name:
                with open(file_name, "w") as f:
                    json.dump(data, f)
            return data
        elif format == "df":
            # Convert to DataFrame
            df = pd.DataFrame(data)
            # optionally remove HTML formatting for relevant columns
            if removeHtml == "yes":
                if "collections" in endpoint:
                    for column in ["collection_summary", "detailed_description", "publications_using", 
                                   "additional_resources", "collection_download_info", "publications_related",
                                   "version_change_log", "collection_acknowledgements"]:
                        if column in df:
                            df[column] = df[column].apply(remove_html_tags)
                elif "analysis" in endpoint:
                    for column in ["result_summary", "detailed_description", "publications_using", 
                                   "additional_resources", "collection_download_info", "publications_related",
                                   "version_change_log", "result_acknowledgements"]:
                        if column in df:
                            df[column] = df[column].apply(remove_html_tags)
                elif "downloads" in endpoint:
                    for column in ["description"]:
                        if column in df:
                            df[column] = df[column].apply(remove_html_tags)
                elif "citations" in endpoint:
                    for column in ["tcia_citation_text, tcia_citation_statement"]:
                        if column in df:
                            df[column] = df[column].apply(remove_html_tags)
                elif "versions" in endpoint:
                    for column in ["version_text"]:
                        if column in df:
                            df[column] = df[column].apply(remove_html_tags)

            # save csv if file name provided
            if file_name:
                df.to_csv(file_name, index=False)
            return df
        else:
            raise ValueError("Invalid format. Please choose 'json', 'df', or 'csv'.")
    else:
        _log.error('Error accessing the API: %s', response.status_code)
        return None


def getCollections(per_page=100, format="", file_name=None, fields=None, ids=None, query=None, removeHtml=None):
    """
    Retrieve collections from the API.

    Args:
        per_page (int, optional): Number of collections per page (default is 100).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
                                 Possible fields: id, date, date_gmt, guid, modified, modified_gmt, slug, status,
                                 type, link, title, featured_media, template, yoast_head, yoast_head_json,
                                 cancer_types, citations, collection_doi, collection_download_info, collection_downloads,
                                 versions, additional_resources, cancer_locations, collection_page_accessibility,
                                 publications_related, version_change_log_archived, collection_status, publications_using,
                                 related_analysis_results, species, version_number, analysis_results, collection_title,
                                 date_updated, subjects, collection_short_title, data_types, detailed_description,
                                 version_change_log, collection_browse_title, supporting_data, collection_featured_image,
                                 collection_summary, collection_acknowledgements, collection_funding,
                                 hide_from_browse_table, program, _links.

    Returns:
        list or DataFrame: Retrieved collections based on the specified parameters and format.

    """
    # set the endpoint for Collections query
    endpoint = "collections/"
    
    # call getQuery to retrieve the data
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query, removeHtml)
    return data


def getAnalyses(per_page=100, format="", file_name=None, fields=None, ids=None, query=None, removeHtml=None):
    """
    Retrieve Analysis Results from the API.

    Args:
        per_page (int, optional): Number of analysis results per page (default is 100).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
                                 Possible fields: id, date, date_gmt, guid, modified, modified_gmt, slug, status,
                                 type, link, title, featured_media, template, yoast_head, yoast_head_json,
                                 cancer_types, citations, result_doi, result_download_info, result_downloads,
                                 version_change_log_archived, versions, additional_resources, cancer_locations,
                                 publications_related, result_page_accessibility, detailed_description,
                                 publications_using, result_title, subjects, version_number, date_updated,
                                 related_collections, result_short_title, supporting_data, collections,
                                 result_browse_title, version_change_log, collection_downloads, result_summary,
                                 result_featured_image, result_acknowledgements, hide_from_browse_table, program,
                                 _links.

    Returns:
        list or DataFrame: Retrieved analysis results based on the specified parameters and format.

    """
    # set the endpoint for an Analysis Result query
    endpoint = "analysis-results/"
    
    # call getQuery to retrieve the data
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query, removeHtml)
    return data


def update_download_file_column(data, max_workers=10):
    """
    Update download_url column by fetching source URLs from WordPress media API in parallel.
    
    Args:
        data: DataFrame containing download_file column
        max_workers: Maximum number of parallel threads (default 10)
    """
    # Define a function to fetch 'source_url' from the API
    def fetch_source_url(media_id):
        try:
            response = requests.get(
                f'https://cancerimagingarchive.net/api/wp/v2/media/{media_id}',
                timeout=10
            )
            if response.status_code == 200:
                media_data = response.json()
                return media_id, media_data.get('source_url', '')
            else:
                _log.warning(f'Failed to fetch media {media_id}: status {response.status_code}')
                return media_id, ''
        except Exception as e:
            _log.error(f'Error fetching media {media_id}: {e}')
            return media_id, ''
    
    # Initialize download_url column if it doesn't exist
    if 'download_url' not in data.columns:
        data['download_url'] = ''
    
    # Extract media IDs that need to be fetched
    media_ids = []
    indices = []
    for idx, download_file_value in enumerate(data['download_file']):
        if isinstance(download_file_value, dict) and 'ID' in download_file_value:
            media_ids.append(download_file_value['ID'])
            indices.append(idx)
    
    if not media_ids:
        _log.info('No media IDs found to fetch')
        return data
    
    _log.info(f'Fetching {len(media_ids)} media URLs in parallel with {max_workers} workers')
    
    # Fetch URLs in parallel
    url_map = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_id = {executor.submit(fetch_source_url, media_id): media_id 
                       for media_id in media_ids}
        
        # Process completed tasks
        completed = 0
        for future in as_completed(future_to_id):
            media_id, url = future.result()
            url_map[media_id] = url
            completed += 1
            if completed % 50 == 0:
                _log.info(f'Completed {completed}/{len(media_ids)} requests')
    
    # Update the DataFrame with fetched URLs
    for idx, media_id in zip(indices, media_ids):
        data.at[idx, 'download_url'] = url_map.get(media_id, '')
    
    _log.info(f'Successfully fetched {len([u for u in url_map.values() if u])} URLs')
    return data


def getDownloads(per_page=200, format="", file_name=None, fields=None, ids=None, query=None, removeHtml=None):
    """
    Retrieve Download metadata from the API.

    Args:
        per_page (int, optional): Number of downloads per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
                                 Possible fields: id, date, date_gmt, guid, modified, modified_gmt, slug, status,
                                 type, link, title, template, yoast_head, yoast_head_json, cancer_type,
                                 download_file, download_requirements, download_size, download_title,
                                 cancer_location, data_license, download_size_unit, download_type, download_url,
                                 data_type, search_url, species, subjects, description, file_type, study_count,
                                 supporting_data, heading_example_text, series_count, download_access, image_count,
                                 collection_status, date_updated, _links.
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).

    Returns:
        list or DataFrame: Retrieved download metadata based on the specified parameters and format.

    """
    # Set the endpoint for a Download query
    endpoint = "downloads/"
    
    # Call getQuery to retrieve the data
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query, removeHtml)
    
    if format == 'df':
        # Check if 'download_file' column exists in the DataFrame
        if 'download_file' in data.columns:
            # Copy download URLs from download_file to download_url column
            data = update_download_file_column(data)
        
    return data


def getCitations(per_page=200, format="", file_name=None, fields=None, ids=None, query=None, removeHtml=None):
    """
    Retrieve Citation metadata from the API.

    Args:
        per_page (int, optional): Number of citations per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
                                 Possible fields: id, date, date_gmt, guid, modified, modified_gmt, slug, status,
                                 type, link, title, template, yoast_head, yoast_head_json, tcia_citation_type,
                                 tcia_citation_text, tcia_citation_statement, tcia_citation_doi, _links.
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).

    Returns:
        list or DataFrame: Retrieved citation metadata based on the specified parameters and format.
    """
    # Set the endpoint for a Citation query
    endpoint = "citations/"
    
    # Call getQuery to retrieve the data
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query, removeHtml)
    return data


def getVersions(per_page=200, format="", file_name=None, fields=None, ids=None, query=None, removeHtml=None):
    """
    Retrieve Version metadata from the API.

    Args:
        per_page (int, optional): Number of citations per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
                                 Possible fields: id, date, date_gmt, guid, modified, modified_gmt, slug,
                                 status, type, link, title, template, yoast_head, yoast_head_json, 
                                 version_number, version_text, version_date, version_downloads, 
                                 related_collection, related_analysis_result, _links.
.
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).

    Returns:
        list or DataFrame: Retrieved citation metadata based on the specified parameters and format.

    """
    # Set the endpoint for a Citation query
    endpoint = "versions/"
    
    # Call getQuery to retrieve the data
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query, removeHtml)
    return data