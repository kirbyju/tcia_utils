"""
CRDC module for tcia_utils to interact with General Commons and Clinical Translational Data Commons APIs
and use Gen3 infrastructure to download data.
"""

from __future__ import annotations

import logging
import json
import os
import re
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Optional, Dict, List, Union
import pandas as pd
from tqdm import tqdm
from tcia_utils.utils import get_proxy

_log = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s',
    level=logging.INFO
)

# Constants
GC_GRAPHQL_ENDPOINT = "https://general.datacommons.cancer.gov/v1/graphql/"
CTDC_GRAPHQL_ENDPOINT = "https://clinical.datacommons.cancer.gov/v1/graphql/"

GC_PHS = "phs004225"
CTDC_PHS = "phs002192"

DRS_PREFIX = "drs://nci-crdc.datacommons.io/"
USER_AGENT = "tcia-crdc-client/1.0"

class Gen3Auth:
    """
    Manages Gen3 API key authentication and caches temporary access tokens.
    """
    def __init__(self, api_key: Optional[str] = None, credentials_file: Optional[Union[str, Path]] = None):
        self.api_key = api_key
        self.tokens: Dict[str, str] = {}  # Caches tokens: commons_host -> access_token

        if not self.api_key and credentials_file:
            path = Path(credentials_file)
            if path.exists():
                try:
                    with path.open("r", encoding="utf-8") as f:
                        data = json.load(f)
                        self.api_key = data.get("api_key")
                except Exception as e:
                    _log.error(f"Failed to read credentials file: {e}")
            else:
                _log.error(f"Credentials file does not exist: {credentials_file}")

    def get_access_token(self, commons_host: str) -> str:
        """
        Retrieves a temporary access token for the given Gen3 commons host, using the cache if valid.
        """
        if not self.api_key:
            raise ValueError("No API key available for Gen3 authentication.")

        if commons_host in self.tokens:
            return self.tokens[commons_host]

        _log.info(f"Fetching temporary Gen3 access token for {commons_host}...")
        url = f"https://{commons_host}/user/credentials/api/access_token"
        payload = json.dumps({"api_key": self.api_key}).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT
            },
            method="POST"
        )

        try:
            proxies = get_proxy()
            if proxies:
                handler = urllib.request.ProxyHandler(proxies)
                opener = urllib.request.build_opener(handler)
                with opener.open(req, timeout=60) as response:
                    res_data = json.loads(response.read().decode("utf-8"))
            else:
                with urllib.request.urlopen(req, timeout=60) as response:
                    res_data = json.loads(response.read().decode("utf-8"))

            token = res_data.get("access_token")
            if not token:
                raise RuntimeError(f"Gen3 authentication response lacks access_token.")

            self.tokens[commons_host] = token
            return token
        except Exception as e:
            _log.error(f"Failed to get Gen3 access token: {e}")
            raise

def query_graphql(
    query: str,
    variables: Optional[Dict[str, Any]] = None,
    endpoint: str = GC_GRAPHQL_ENDPOINT
) -> Dict[str, Any]:
    """
    Executes an arbitrary GraphQL query on the specified endpoint.
    """
    payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT
        },
        method="POST"
    )

    try:
        proxies = get_proxy()
        if proxies:
            handler = urllib.request.ProxyHandler(proxies)
            opener = urllib.request.build_opener(handler)
            with opener.open(req, timeout=120) as response:
                result = json.loads(response.read().decode("utf-8"))
        else:
            with urllib.request.urlopen(req, timeout=120) as response:
                result = json.loads(response.read().decode("utf-8"))

        if "errors" in result:
            messages = "; ".join(error.get("message", str(error)) for error in result["errors"])
            raise RuntimeError(f"GraphQL Query errors: {messages}")
        return result.get("data", {})
    except Exception as e:
        _log.error(f"GraphQL Query failed: {e}")
        raise

def get_studies(
    phs_accession: str = GC_PHS,
    endpoint: str = GC_GRAPHQL_ENDPOINT,
    first: int = 10000,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """
    Convenience function to list studies for a given PHS accession.
    """
    query = """
    query TCIAStudies($phs: [String], $first: Int, $offset: Int) {
      studies(phs_accessions: $phs, first: $first, offset: $offset) {
        phs_accession
        study_acronym
        study_name
        study_description
        program_name
      }
    }
    """
    variables = {
        "phs": [phs_accession],
        "first": first,
        "offset": offset
    }
    res = query_graphql(query, variables, endpoint)
    return res.get("studies", [])

def get_counts(
    phs_accession: str = GC_PHS,
    endpoint: str = GC_GRAPHQL_ENDPOINT
) -> Dict[str, Any]:
    """
    Convenience function to get node counts for a PHS accession.
    """
    # Query introspect root fields first to only query valid fields
    introspect_query = """
    query {
      __type(name: "Query") {
        fields {
          name
        }
      }
    }
    """
    intro_res = query_graphql(introspect_query, {}, endpoint)
    fields = [f["name"] for f in intro_res.get("__type", {}).get("fields", [])]

    # Desired fields
    desired = [
        "participantsCount",
        "samplesCount",
        "filesCount",
        "diagnosesCount",
        "treatmentsCount",
        "imagesCount",
        "genomicInfoCount",
        "proteomicsCount",
        "pdxCount",
        "multiplexMicroscopiesCount",
        "nonDICOMCTimagesCount",
        "nonDICOMMRimagesCount",
        "nonDICOMPETimagesCount",
        "nonDICOMpathologyImagesCount",
        "nonDICOMradiologyAllModalitiesCount",
    ]

    valid_fields = [f for f in desired if f in fields]
    if not valid_fields:
        return {}

    count_lines = [f"{field}: {field}(phs_accession: $phs)" for field in valid_fields]
    query = f"""
    query TCIACounts($phs: String!) {{
      {" ".join(count_lines)}
    }}
    """
    variables = {"phs": phs_accession}
    return query_graphql(query, variables, endpoint)

def get_file_overview(
    phs_accession: str = GC_PHS,
    studies: Optional[List[str]] = None,
    endpoint: str = GC_GRAPHQL_ENDPOINT,
    first: int = 10000,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """
    Convenience function to retrieve file overview details.
    """
    query = """
    query TCIAFileOverview($phs: [String], $studies: [String], $first: Int, $offset: Int) {
      fileOverview(
        phs_accession: $phs
        studies: $studies
        first: $first
        offset: $offset
      ) {
        file_name
        file_type
        file_size
        file_id
        accesses
        phs_accession
        study_acronym
        study_data_type
        image_modality
      }
    }
    """
    variables = {
        "phs": [phs_accession],
        "studies": studies,
        "first": first,
        "offset": offset
    }
    res = query_graphql(query, variables, endpoint)
    return res.get("fileOverview", [])

def download_file_by_drs(
    drs_uri: str,
    output_dir: Union[str, Path] = "crdcDownload",
    auth: Optional[Gen3Auth] = None,
    file_name: Optional[str] = None
) -> Path:
    """
    Downloads a single file using its DRS URI or File ID via the Gen3 framework.
    """
    # Standardize DRS URI
    drs_uri = drs_uri.strip()
    if not drs_uri.startswith("drs://"):
        drs_uri = DRS_PREFIX + drs_uri.removeprefix(DRS_PREFIX)

    parsed = urllib.parse.urlparse(drs_uri)
    commons_host = parsed.netloc
    object_id = parsed.path.lstrip("/")

    # Get signed download URL
    url = f"https://{commons_host}/user/data/download/{urllib.parse.quote(object_id)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    if auth:
        token = auth.get_access_token(commons_host)
        req.add_header("Authorization", f"Bearer {token}")

    try:
        proxies = get_proxy()
        if proxies:
            handler = urllib.request.ProxyHandler(proxies)
            opener = urllib.request.build_opener(handler)
            with opener.open(req, timeout=120) as response:
                res_data = json.loads(response.read().decode("utf-8"))
        else:
            with urllib.request.urlopen(req, timeout=120) as response:
                res_data = json.loads(response.read().decode("utf-8"))

        download_url = res_data.get("url")
        if not download_url:
            # Sometime response is just the string of the url itself if it can't decode as json
            raise ValueError("Could not get signed download URL.")
    except json.JSONDecodeError:
        # Fallback if body is not JSON but raw URL string
        try:
            if proxies:
                with opener.open(req, timeout=120) as response:
                    download_url = response.read().decode("utf-8").strip()
            else:
                with urllib.request.urlopen(req, timeout=120) as response:
                    download_url = response.read().decode("utf-8").strip()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch signed download URL: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to resolve signed URL from Gen3: {e}")

    # Ensure output directory exists
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    if not file_name:
        # Parse from download URL or object ID
        file_name = Path(urllib.parse.urlparse(download_url).path).name or object_id

    final_dest = out_path / file_name
    temp_dest = final_dest.with_suffix(".tmp")

    _log.info(f"Downloading DRS URI {drs_uri} to {final_dest}...")
    try:
        if proxies:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler(proxies))
            with opener.open(download_url, timeout=1800) as response:
                with temp_dest.open("wb") as out_f:
                    out_f.write(response.read())
        else:
            with urllib.request.urlopen(download_url, timeout=1800) as response:
                with temp_dest.open("wb") as out_f:
                    out_f.write(response.read())

        if temp_dest.exists():
            temp_dest.replace(final_dest)
        return final_dest
    except Exception as e:
        if temp_dest.exists():
            temp_dest.unlink()
        raise RuntimeError(f"Failed to download file payload: {e}")

def parse_manifest(manifest_path: Union[str, Path]) -> List[str]:
    """
    Parses a CSV, TSV, or XLSX manifest and extracts all DRS URIs or File IDs.
    """
    path = Path(manifest_path)
    if not path.exists():
        raise FileNotFoundError(f"Manifest file does not exist: {manifest_path}")

    ext = path.suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(path)
    elif ext == ".tsv":
        df = pd.read_csv(path, sep="\t")
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_excel(path)
    else:
        raise ValueError(f"Unsupported manifest file format: {ext}")

    # Find relevant columns
    cols = [col.lower().strip() for col in df.columns]
    target_idx = -1
    for target in ["drs_uri", "drsuri", "file_id", "fileid", "file id"]:
        if target in cols:
            target_idx = cols.index(target)
            break

    if target_idx == -1:
        raise ValueError(f"No columns matching 'drs_uri' or 'file_id' found in the manifest.")

    col_name = df.columns[target_idx]
    # Filter empty values and return list
    drs_list = df[col_name].dropna().astype(str).tolist()
    return [drs.strip() for drs in drs_list if drs.strip()]

def download_data(
    data: Union[str, Path, pd.DataFrame, List[str]],
    output_dir: Union[str, Path] = "crdcDownload",
    auth: Optional[Gen3Auth] = None,
    max_workers: int = 5
) -> List[Path]:
    """
    Downloads data from Gen3/CRDC using list of DRS URIs/File IDs, DataFrame, or manifest file.
    """
    drs_uris: List[str] = []

    if isinstance(data, list):
        drs_uris = [drs.strip() for drs in data if drs.strip()]
    elif isinstance(data, pd.DataFrame):
        df = data
        cols = [col.lower().strip() for col in df.columns]
        target_idx = -1
        for target in ["drs_uri", "drsuri", "file_id", "fileid", "file id"]:
            if target in cols:
                target_idx = cols.index(target)
                break
        if target_idx == -1:
            raise ValueError(f"No columns matching 'drs_uri' or 'file_id' found in the DataFrame.")
        col_name = df.columns[target_idx]
        drs_uris = df[col_name].dropna().astype(str).tolist()
    elif isinstance(data, (str, Path)):
        # Treat as file manifest path
        drs_uris = parse_manifest(data)
    else:
        raise TypeError(f"Invalid type for input data: {type(data)}")

    if not drs_uris:
        _log.warning("No files found to download.")
        return []

    _log.info(f"Preparing to download {len(drs_uris)} files with {max_workers} workers...")
    downloaded_paths: List[Path] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_file_by_drs, drs, output_dir, auth): drs
            for drs in drs_uris
        }

        # Use tqdm to show real-time progress
        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading"):
            drs = futures[future]
            try:
                path = future.result()
                downloaded_paths.append(path)
            except Exception as e:
                _log.error(f"Failed to download {drs}: {e}")

    return downloaded_paths
