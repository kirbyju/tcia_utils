import pandas as pd
import requests
from datetime import datetime
import logging

_log = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s'
    , level=logging.INFO
)

####### getDoi function
# Gets metadata for one or more DOIs
# Returns all TCIA DOIs if no parameters are specified
# query parameter searches all metadata fields
# created parameter expects a 4 digit year
# license parameter requires an exact match. currently supported licenses:
#    ['cc-by-3.0', 'cc-by-4.0', 'cc-by-nc-4.0', 'nctn data archive license',
#    'tcia limited access license', 'cc-by-nc-3.0']
# See https://support.datacite.org/docs/api-get-doi for more details

def getDoi(query = "",
           created = "",
           license = "",
           format = ""):

    datacite_url = "https://api.datacite.org/dois/"
    datacite_headers = {"accept": "application/vnd.api+json"}
    df = pd.DataFrame()

    # set query parameters
    options = {}
    options['provider-id'] = "tciar"
    options['page[size]'] = 1000
    if query:
        options['query'] = query
    if created:
        options['created'] = created
    if license:
        options['license'] = license
    _log.info(f'Calling... {datacite_url} with parameters {options}')

    try:
        data = requests.get(datacite_url, headers = datacite_headers, params = options)
        data.raise_for_status()

        # check for empty results and format output
        if data.text != "":
            data = data.json()
            # format the output (optional)
            if format == "df" or format == "csv":
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
                        rights = item["attributes"]["rightsList"][0]["rights"]
                    except (KeyError, IndexError):
                        rights = None
                    try:
                        rights_uri = item["attributes"]["rightsList"][0]["rightsUri"]
                    except (KeyError, IndexError):
                        rights_uri = None
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
                                "CreatorNames": ", ".join(creator_names),
                                "Title": title, 
                                "Created": created, 
                                "Updated": updated, 
                                "Related": related, 
                                "Version": version, 
                                "Rights": rights, 
                                "RightsURI": rights_uri, 
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
                return data
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