import pandas as pd
import requests
from datetime import datetime
import time
import logging
from tcia_utils.utils import searchDf
from tcia_utils.utils import copy_df_cols


_log = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s'
    , level=logging.INFO
)

def getDoi(format = ""):
    """ 
        Gets metadata for all TCIA DOIs.
        Returns a dataframe by default, but format can be set to CSV or JSON.
        See https://support.datacite.org/docs/api-get-doi for more details.
    """
    datacite_url = "https://api.datacite.org/dois/"
    datacite_headers = {"accept": "application/vnd.api+json"}
    df = pd.DataFrame()

    # set query parameters
    options = {}
    options['provider-id'] = "tciar"
    options['page[size]'] = 1000
    _log.info(f'Calling... {datacite_url} with parameters {options}')

    try:
        data = requests.get(datacite_url, headers = datacite_headers, params = options)
        data.raise_for_status()

        # check for empty results and format output
        if data.text != "":
            data = data.json()
            # format the output (optional)
            if format == "json":
                return data
            else:
                dois = []
                for item in data["data"]:
                    doi = item["id"]
                    try:
                        identifier = item["attributes"]["identifiers"][0]["identifier"]
                    except (KeyError, IndexError):
                        identifier = None
                    creators = item["attributes"]["creators"]
                    creator_names = []
                    for creator in creators:
                        given_name = creator.get("givenName")
                        family_name = creator.get("familyName")
                        name = creator.get("name")
                        if given_name and family_name:
                            creator_name = f"{given_name} {family_name}"
                        elif name:
                            creator_name = name
                        else:
                            creator_name = ""
                        name_identifiers = creator.get("nameIdentifiers")
                        name_identifier_str = ""
                        if name_identifiers:
                            name_identifier_str = f" ({', '.join([x.get('nameIdentifier') for x in name_identifiers])})"
                        creator_name += name_identifier_str
                        creator_names.append(creator_name)
                    title = item["attributes"]["titles"][0]["title"]
                    created = item["attributes"]["created"]
                    updated = item["attributes"]["updated"]
                    try:
                        relation_type = item["attributes"]["relatedIdentifiers"][0]["relationType"]
                    except (KeyError, IndexError):
                        relation_type = None
                    try:
                        related_identifier = item["attributes"]["relatedIdentifiers"][0]["relatedIdentifier"]
                    except (KeyError, IndexError):
                        related_identifier = None
                    version = item["attributes"]["version"]
                    try:
                        rights = item["attributes"]["rightsList"]
                        rights_list = [r["rights"] for r in rights]
                        rights_uri_list = [r["rightsUri"] for r in rights]
                    except (KeyError, IndexError):
                        rights_list = []
                        rights_uri_list = []
                    try:
                        description = item["attributes"]["descriptions"][0]["description"]
                    except (KeyError, IndexError):
                        description = None
                    try:
                        funding_references = item["attributes"]["fundingReferences"]
                    except KeyError:
                        funding_references = None
                    url = item["attributes"]["url"]
                    citation_count = item["attributes"]["citationCount"]
                    reference_count = item["attributes"]["referenceCount"]
                    related = f"{relation_type}: {related_identifier}" if relation_type and related_identifier else None
                    dois.append({"DOI": doi, 
                                "Identifier": identifier, 
                                "CreatorNames": "; ".join(creator_names),
                                "Title": title, 
                                "Created": created, 
                                "Updated": updated, 
                                "Related": related, 
                                "Version": version, 
                                "Rights": "; ".join(rights_list),
                                "RightsURI": "; ".join(rights_uri_list),
                                "Description": description, 
                                "FundingReferences": funding_references, 
                                "URL": url, 
                                "CitationCount": citation_count, 
                                "ReferenceCount": reference_count})

                df = pd.DataFrame(dois, columns=["DOI", "Identifier", "CreatorNames", "Title", "Created", "Updated", "Related", 
                                                  "Version", "Rights", "RightsURI", "Description", "FundingReferences", "URL", 
                                                  "CitationCount", "ReferenceCount"])  
                if format == "csv":
                    now = datetime.now()
                    dt_string = now.strftime("%Y-%m-%d_%H%M")
                    df.to_csv('datacite_' + dt_string + '.csv')
                    _log.info(f"Report saved as datacite_{dt_string}.csv")
                return df
        else:
            _log.info(f'No results found.')
            
    # handle errors
    except requests.exceptions.HTTPError as errh:
        _log.error(f'Error: {errh}')
    except requests.exceptions.ConnectionError as errc:
        _log.error(f'Error: {errc}')
    except requests.exceptions.Timeout as errt:
        _log.error(f'Error: {errt}')
    except requests.exceptions.RequestException as err:
        _log.error(f'Error: {err}')
        
        
def getDerivedDois(dois, format='df'):
    """
    Retrieve datasets that are derived from a given list of DOIs or a single DOI using the DataCite API.

    This function queries the DataCite API to find datasets that have a "IsDerivedFrom" relationship
    with each DOI in the provided list or the single DOI provided. The results can be returned in various formats: 
    a pandas DataFrame, a CSV string, or a list of dictionaries (JSON).

    Parameters:
    -----------
    dois : list of str or str
        A list of Digital Object Identifiers (DOIs) or a single DOI as a string to search for derived datasets.
    format : str, optional
        The format in which to return the results. Options are:
        - 'df' (default): Return results as a pandas DataFrame.
        - 'csv': Return results as a CSV string.
        - 'json': Return results as a list of dictionaries.

    Returns:
    --------
    pandas.DataFrame or str or list of dict
        The derived datasets in the specified format:
        - If format='df', returns a pandas DataFrame.
        - If format='csv', returns a CSV string.
        - If format='json', returns a list of dictionaries.

    Example:
    --------
    >>> dois = ["10.1234/example.doi1", "10.5678/example.doi2"]
    >>> df = getDerivedDois(dois, format='df')
    >>> print(df)
    
    >>> single_doi = "10.1234/example.doi1"
    >>> df = getDerivedDois(single_doi, format='df')
    >>> print(df)
    """
    if isinstance(dois, str):
        dois = [dois]
    
    base_url = "https://api.datacite.org/works"
    all_datasets = []

    for doi in dois:
        query = f'relatedIdentifiers.relatedIdentifierType:DOI AND relatedIdentifiers.relatedIdentifier:{doi} AND relatedIdentifiers.relationType:IsDerivedFrom'
        params = {
            'query': query,
            'rows': 1000  # Adjust as needed for pagination
        }
        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            data = response.json()
            if 'data' in data:
                datasets = data['data']
                for dataset in datasets:
                    dataset['search_related_identifier'] = doi
                all_datasets.extend(datasets)
                _log.info(f"Found {len(datasets)} datasets derived from DOI: {doi}")
            else:
                _log.info(f"No datasets found derived from DOI: {doi}")
        else:
            _log.error(f"Failed to retrieve data for DOI: {doi}, Error: {response.text}")
    
    if format == 'df':
        df = pd.json_normalize(all_datasets)
        return df
    elif format == 'csv':
        df = pd.json_normalize(all_datasets)
        csv_data = df.to_csv(index=False)
        return csv_data
    elif format == 'json':
        return all_datasets
    else:
        _log.error(f"Invalid format specified. Use 'df', 'csv', or 'json'.")

        
def getGithubMentions(token, delay=10, resume_from=None):
    """
    Search GitHub for mentions of TCIA DOIs and return a DataFrame with the results.

    This function creates a DataFrame containing TCIA DOIs, searches GitHub for mentions of these DOIs or their Titles,
    and returns a DataFrame with the search results. It supports resuming from a specific DOI and includes a delay
    between API requests to avoid rate limiting.

    Parameters:
    token (str): Personal access token for GitHub API authentication.
    delay (int, optional): Delay in seconds between API requests to avoid rate limiting. Default is 1 second.
    resume_from (str, optional): DOI to resume the search from. Default is None.

    Returns:
    pd.DataFrame: DataFrame containing the search results with an additional 'DOI' column.
    """
    # get current TCIA DOIs
    datacite_df = getDoi()
    
    # Extract unique DOIs from both columns
    unique_dois = pd.unique(datacite_df[['Identifier', 'DOI']].values.ravel('K'))
    
    # Initialize an empty DataFrame for the results
    tcia_mentions = pd.DataFrame()
    
    # Determine the starting index if resuming
    start_index = 0
    if resume_from:
        start_index = unique_dois.tolist().index(resume_from)
    
    # Iterate over each unique DOI starting from the resume point
    while start_index < len(unique_dois):
        doi = unique_dois[start_index]
        _log.info(f'Starting search for DOI: {doi}')  
        
        # Construct the search query
        query = f'"{doi}" in:file'
        url = f'https://api.github.com/search/code?q={query}'
        
        # Set up the headers with your personal token for authentication
        headers = {
            'Authorization': f'token {token}'
        }
        
        try:
            # Make the request to the GitHub API
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the response JSON
                search_results = response.json()
                # Convert the items key to a DataFrame
                df = pd.DataFrame(search_results['items'])
                # Add a column for the DOI
                df['DOI'] = doi
                # Concatenate the results to the tcia_mentions DataFrame
                tcia_mentions = pd.concat([tcia_mentions, df], ignore_index=True)
                # Move to the next DOI
                start_index += 1
            else:
                _log.warning(f'Failed to fetch data for DOI {doi}: {response.status_code}')
            
            time.sleep(delay)  # Delay the next request by the specified number of seconds
        except Exception as e:
            _log.error(f'An error occurred: {e}')
            # Do not increment start_index, retry the same DOI
    
    # Define a function to extract nested information of interest in certain fields
    def extract_info(record):
        full_name = record.get('full_name', '')
        login = record['owner'].get('login', '') if 'owner' in record else ''
        description = record.get('description', '')
        return full_name, login, description

    # Apply the function to the 'repository' column and create new columns
    tcia_mentions[['full_name', 'login', 'description']] = tcia_mentions['repository'].apply(
        lambda record: pd.Series(extract_info(record))
    )

    # Drop unnecessary columns
    columns_to_drop = ['name', 'repository', 'sha', 'url', 'git_url', 'score']
    tcia_mentions = tcia_mentions.drop(columns_to_drop, axis = 1)
    
    # Reorder the columns
    columns_order = ['DOI', 'login', 'full_name', 'path', 'html_url', 'description']
    tcia_mentions = tcia_mentions[columns_order]
    
    # Save the DataFrame to an Excel file
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_name = f'tcia_github_mentions_{date_str}.xlsx'
    tcia_mentions.to_excel(file_name, index=False)
    
    return tcia_mentions
