import pandas as pd
import json
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tcia_utils.utils import searchDf
from tcia_utils.utils import remove_html_tags
from tcia_utils.utils import copy_df_cols

_log = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s'
    , level=logging.INFO
)

base_url = "https://cancerimagingarchive.net/api/v2/"

def getQuery(endpoint, per_page, format="", file_name=None, fields=None, ids=None, query=None,
             removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
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
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved results based on the specified parameters and format.

    Raises:
        ValueError: If an invalid format is provided.

    """
    # Set base URL based on version
    current_base_url = base_url if api_version == "v2" else "https://cancerimagingarchive.net/api/v1/"

    # Append custom fields to the endpoint if provided
    if fields:
        fields_str = ','.join(fields)
        field_param = "fields" if api_version == "v2" else "_fields"
        endpoint += f"?{field_param}={fields_str}"
    
    # Set URL 
    url = current_base_url + endpoint
    
    # Set the request parameters
    params = {'per_page': per_page}
    
    # Add ids to the parameters if provided
    if ids:
        ids_str = ','.join(str(id) for id in ids)
        if api_version == "v2":
            params['id'] = ids_str
        else:
            params['include'] = ids_str
    
    # Add query to the parameters if provided
    if query:
        params['search'] = query

    # Add v2 specific parameters
    if api_version == "v2":
        if verbose:
            params['v'] = 1
        if orderby:
            params['orderby'] = orderby
        if order:
            params['order'] = order
    
    # Make a GET request to the API endpoint with the parameters
    _log.info('Requesting %s', url)
    response = requests.get(url, params=params)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        response_data = response.json()
        
        if api_version == "v2":
            data = response_data.get('results', [])
            total_pages = response_data.get('total_pages', 1)
            current_page = response_data.get('page', 1)

            if current_page < total_pages:
                pages_to_fetch = range(current_page + 1, total_pages + 1)
                _log.info('Requesting %s additional pages in parallel', len(pages_to_fetch))

                def fetch_page(page_num):
                    page_params = params.copy()
                    page_params['page'] = page_num
                    res = requests.get(url, params=page_params)
                    if res.status_code == 200:
                        return page_num, res.json().get('results', [])
                    else:
                        _log.error('Error accessing the API page %s: %s', page_num, res.status_code)
                        return page_num, []

                results_map = {}
                with ThreadPoolExecutor(max_workers=10) as executor:
                    future_to_page = {executor.submit(fetch_page, p): p for p in pages_to_fetch}
                    for future in as_completed(future_to_page):
                        page_num, page_data = future.result()
                        results_map[page_num] = page_data

                for p in sorted(results_map.keys()):
                    data.extend(results_map[p])
        else:
            # v1 logic
            data = response_data
            # Check if there are more pages to fetch using Link header
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
                    cols = ["collection_summary", "detailed_description", "publications_using",
                            "additional_resources", "collection_download_info", "publications_related",
                            "version_change_log", "collection_acknowledgements", "collection_abstract",
                            "collection_introduction", "collection_funding", "subject_inclusion_and_exclusion_criteria",
                            "data_acquisition", "data_analysis", "usage_notes"]
                    for column in cols:
                        if column in df:
                            df[column] = df[column].apply(remove_html_tags)
                elif "analysis" in endpoint:
                    cols = ["result_summary", "detailed_description", "publications_using",
                            "additional_resources", "collection_download_info", "publications_related",
                            "version_change_log", "result_acknowledgements", "result_abstract",
                            "result_introduction", "result_funding"]
                    for column in cols:
                        if column in df:
                            df[column] = df[column].apply(remove_html_tags)
                elif "downloads" in endpoint:
                    for column in ["description"]:
                        if column in df:
                            df[column] = df[column].apply(remove_html_tags)
                elif "citations" in endpoint:
                    for column in ["tcia_citation_text", "tcia_citation_statement"]:
                        if column in df:
                            df[column] = df[column].apply(remove_html_tags)
                elif "versions" in endpoint:
                    for column in ["version_text"]:
                        if column in df:
                            df[column] = df[column].apply(remove_html_tags)
                elif "licenses" in endpoint:
                    for column in ["content"]:
                        if column in df:
                            df[column] = df[column].apply(remove_html_tags)
                elif "requirements" in endpoint:
                    for column in ["requirements_text"]:
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


def getCollections(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                   removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve collections from the API.

    Args:
        per_page (int, optional): Number of collections per page (default is 50).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
                                 v2 Possible fields: id, slug, collection_doi, collection_title, collection_short_title,
                                 collection_summary, collection_abstract, detailed_description,
                                 collection_page_accessibility, collection_acknowledgements, program,
                                 collection_featured_image, collection_funding, cancer_types, cancer_locations,
                                 data_types, citations, collection_downloads, related_analysis_results, species,
                                 related_collection, version_number, date_updated, subjects, supporting_data,
                                 analysis_results, _links, wordpress_featured_image.
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved collections based on the specified parameters and format.

    """
    # set the endpoint for Collections query
    endpoint = "collections/"
    
    # call getQuery to retrieve the data
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getCancerTypes(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                   removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve Cancer Type metadata from the API.

    Args:
        per_page (int, optional): Number of results per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved cancer type metadata based on the specified parameters and format.

    """
    endpoint = "cancer-types/"
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getCancerLocations(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                       removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve Cancer Location metadata from the API.

    Args:
        per_page (int, optional): Number of results per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved cancer location metadata based on the specified parameters and format.

    """
    endpoint = "cancer-locations/"
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getSpecies(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
               removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve Species metadata from the API.

    Args:
        per_page (int, optional): Number of results per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved species metadata based on the specified parameters and format.

    """
    endpoint = "species/"
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getDataTypes(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                 removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve Data Type metadata from the API.

    Args:
        per_page (int, optional): Number of results per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved data type metadata based on the specified parameters and format.

    """
    endpoint = "data-types/"
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getSupportingData(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                      removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve Supporting Data metadata from the API.

    Args:
        per_page (int, optional): Number of results per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved supporting data metadata based on the specified parameters and format.

    """
    endpoint = "supporting-data/"
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getFileTypes(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                 removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve File Type metadata from the API.

    Args:
        per_page (int, optional): Number of results per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved file type metadata based on the specified parameters and format.

    """
    endpoint = "file-types/"
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getLicenses(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve License metadata from the API.

    Args:
        per_page (int, optional): Number of results per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved license metadata based on the specified parameters and format.

    """
    endpoint = "licenses/"
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getDownloadRequirements(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                             removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve Download Requirement metadata from the API.

    Args:
        per_page (int, optional): Number of results per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved download requirement metadata based on the specified parameters and format.

    """
    endpoint = "download-requirements/"
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getPrograms(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve Program metadata from the API.

    Args:
        per_page (int, optional): Number of results per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved program metadata based on the specified parameters and format.

    """
    endpoint = "programs/"
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getVersionDownloads(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                        removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve Version Download metadata from the API.

    Args:
        per_page (int, optional): Number of results per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved version download metadata based on the specified parameters and format.

    """
    endpoint = "version_downloads/"
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getAnalyses(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve Analysis Results from the API.

    Args:
        per_page (int, optional): Number of analysis results per page (default is 50).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
                                 v2 Possible fields: id, slug, result_title, result_short_title, result_summary,
                                 detailed_description, result_page_accessibility, result_acknowledgements,
                                 program, result_featured_image, result_funding, cancer_types, cancer_locations,
                                 species, subjects, supporting_data, citations, collection_downloads,
                                 collections, related_analysis_results, related_collections, version_number,
                                 date_updated, _links, wordpress_featured_image, result_doi.
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved analysis results based on the specified parameters and format.

    """
    # set the endpoint for an Analysis Result query
    endpoint = "analysis-results/"
    
    # call getQuery to retrieve the data
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getDownloads(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                 removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve Download metadata from the API.

    Args:
        per_page (int, optional): Number of downloads per page (default is 50).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
                                 v2 Possible fields: id, slug, download_title, download_type, data_type,
                                 file_type, download_access, collection_status, date_updated, download_file,
                                 download_url, fill_download_specs, download_requirements, data_license,
                                 search_url, description, supporting_data, download_size, download_size_unit,
                                 subjects, study_count, series_count, image_count, download_metadata.
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved download metadata based on the specified parameters and format.

    """
    # Set the endpoint for a Download query
    endpoint = "downloads/"
    
    # Call getQuery to retrieve the data
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getCitations(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                 removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve Citation metadata from the API.

    Args:
        per_page (int, optional): Number of citations per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved citation metadata based on the specified parameters and format.
    """
    # Set the endpoint for a Citation query
    endpoint = "citations/"
    
    # Call getQuery to retrieve the data
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data


def getVersions(per_page=50, format="", file_name=None, fields=None, ids=None, query=None,
                removeHtml=None, api_version="v2", verbose=False, orderby=None, order=None):
    """
    Retrieve Version metadata from the API.

    Args:
        per_page (int, optional): Number of results per page (default is 200).
        format (str, optional): Output format ('json' or 'df') (default is JSON if not populated).
        file_name (str, optional): File name to save the output as JSON or CSV if format = "df".
        fields (list, optional): List of custom fields to include in the return values (default is None).
        ids (list, optional): List of IDs to include in the request (default is None).
        query (str, optional): Search criteria to filter results (default is None).
        removeHtml (str, optional): If "yes", removes HTML tags from relevant columns in DataFrame output.
        api_version (str, optional): API version to use ('v1' or 'v2') (default is 'v2').
        verbose (bool, optional): If True, returns full content for longer fields in v2 (default is False).
        orderby (str, optional): Field slug to sort by (v2 only).
        order (str, optional): Sort order ('asc' or 'desc') (v2 only).

    Returns:
        list or DataFrame: Retrieved version metadata based on the specified parameters and format.

    """
    # Set the endpoint for a Version query
    endpoint = "versions/"
    
    # Call getQuery to retrieve the data
    data = getQuery(endpoint, per_page, format, file_name, fields, ids, query,
                    removeHtml, api_version, verbose, orderby, order)
    return data