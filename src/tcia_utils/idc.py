import logging
import pandas as pd
from typing import Union, List, Optional
from datetime import datetime
from idc_index import IDCClient
from IPython.display import HTML
import os
import plotly.express as px
from tcia_utils.utils import format_disk_space
from tcia_utils.utils import get_proxy
import csv
import warnings
import requests
import io
import pydicom

_log = logging.getLogger(__name__)

# Global client instance
_client = None

def get_client():
    global _client
    if _client is None:
        _client = IDCClient.client()
        # Ensure index is registered in duckdb upon initialization
        _client.sql_query("SELECT 1 FROM index LIMIT 1")
    return _client

# Mapping IDC columns to NBIA columns
COLUMN_MAPPING = {
    'collection_id': 'Collection',
    'analysis_result_id': 'AnalysisResult',
    'PatientID': 'PatientID',
    'SeriesInstanceUID': 'SeriesInstanceUID',
    'StudyInstanceUID': 'StudyInstanceUID',
    'Modality': 'Modality',
    'BodyPartExamined': 'BodyPartExamined',
    'Manufacturer': 'Manufacturer',
    'ManufacturerModelName': 'ManufacturerModelName',
    'StudyDate': 'StudyDate',
    'SeriesDate': 'SeriesDate',
    'SeriesDescription': 'SeriesDescription',
    'SeriesNumber': 'SeriesNumber',
    'instanceCount': 'ImageCount',
    'license_short_name': 'LicenseName',
    'source_DOI': 'DataDescriptionURI',
    'series_size_MB': 'FileSize',
    'count': 'Count'
}

def format_output(df: pd.DataFrame, format: str = "json", max_rows: int = 20):
    if df.empty:
        if format == "json":
            return []
        return df

    # Standardize column names
    df = df.rename(columns=COLUMN_MAPPING)

    if format == "df":
        return df
    elif format == "csv":
        csv_filename = f"idc_query_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv"
        df.to_csv(csv_filename, index=False)
        _log.info(f"CSV saved to: {csv_filename}")
        return df
    elif format == "html":
        return idcOhifViewer(df, max_rows=max_rows)
    else: # default json
        return df.to_dict(orient='records')

def _apply_common_filters(query: str, params: list,
                        collection: str = "", analysisResult: str = "", doi: str = "",
                        age: str = "", sex: str = "", studyDesc: str = "",
                        license: str = "", modality: str = "", bodyPart: str = "",
                        manufacturer: str = "", seriesDesc: str = ""):
    if collection:
        query += " AND collection_id ILIKE ?"
        params.append(collection)
    if analysisResult:
        query += " AND analysis_result_id ILIKE ?"
        params.append(analysisResult)
    if doi:
        query += " AND source_DOI ILIKE ?"
        params.append(doi)
    if age:
        query += " AND PatientAge ILIKE ?"
        params.append(age)
    if sex:
        query += " AND PatientSex ILIKE ?"
        params.append(sex)
    if studyDesc:
        query += " AND StudyDescription ILIKE ?"
        params.append(studyDesc)
    if license:
        query += " AND license_short_name ILIKE ?"
        params.append(license)
    if modality:
        query += " AND Modality ILIKE ?"
        params.append(modality)
    if bodyPart:
        query += " AND BodyPartExamined ILIKE ?"
        params.append(bodyPart)
    if manufacturer:
        query += " AND Manufacturer ILIKE ?"
        params.append(manufacturer)
    if seriesDesc:
        query += " AND SeriesDescription ILIKE ?"
        params.append(seriesDesc)
    return query, params

def getCollections(analysisResult: str = "", doi: str = "", age: str = "", sex: str = "",
                   studyDesc: str = "", license: str = "", modality: str = "",
                   bodyPart: str = "", manufacturer: str = "", seriesDesc: str = "",
                   format: str = ""):
    """
    Gets a list of collections.
    Allows filtering by analysis result, DOI, patient age, patient sex,
    study description, license, modality, body part, manufacturer, and series description.
    """
    client = get_client()
    query = "SELECT DISTINCT collection_id FROM index WHERE 1=1"
    params = []
    query, params = _apply_common_filters(
        query, params, analysisResult=analysisResult, doi=doi, age=age, sex=sex,
        studyDesc=studyDesc, license=license, modality=modality, bodyPart=bodyPart,
        manufacturer=manufacturer, seriesDesc=seriesDesc)

    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getAnalysisResults(collection: str = "", doi: str = "", age: str = "", sex: str = "",
                       studyDesc: str = "", license: str = "", modality: str = "",
                       bodyPart: str = "", manufacturer: str = "", seriesDesc: str = "",
                       format: str = ""):
    """
    Gets a list of analysis results.
    Allows filtering by collection, DOI, patient age, patient sex,
    study description, license, modality, body part, manufacturer, and series description.
    """
    client = get_client()
    query = "SELECT DISTINCT analysis_result_id FROM index WHERE 1=1"
    params = []
    query, params = _apply_common_filters(
        query, params, collection=collection, doi=doi, age=age, sex=sex,
        studyDesc=studyDesc, license=license, modality=modality, bodyPart=bodyPart,
        manufacturer=manufacturer, seriesDesc=seriesDesc)

    df = client._duckdb_conn.execute(query, params).df()
    df = df[df['analysis_result_id'].notna()]
    return format_output(df, format=format)

def getBodyPart(collection: str = "", modality: str = "", format: str = ""):
    """
    Gets Body Part Examined metadata.
    Allows filtering by collection and modality.
    """
    client = get_client()
    query = "SELECT DISTINCT BodyPartExamined FROM index WHERE 1=1"
    params = []
    if collection:
        query += " AND collection_id ILIKE ?"
        params.append(collection)
    if modality:
        query += " AND Modality ILIKE ?"
        params.append(modality)

    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getBodyPartCounts(collection: str = "", modality: str = "", format: str = ""):
    """
    Gets counts of Body Part metadata.
    Allows filtering by collection and modality.
    """
    client = get_client()
    query = "SELECT BodyPartExamined, COUNT(DISTINCT SeriesInstanceUID) as count FROM index WHERE 1=1"
    params = []
    if collection:
        query += " AND collection_id ILIKE ?"
        params.append(collection)
    if modality:
        query += " AND Modality ILIKE ?"
        params.append(modality)
    query += " GROUP BY BodyPartExamined"
    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getModality(collection: str = "", bodyPart: str = "", format: str = ""):
    """
    Gets Modalities metadata.
    Allows filtering by collection and bodyPart.
    """
    client = get_client()
    query = "SELECT DISTINCT Modality FROM index WHERE 1=1"
    params = []
    if collection:
        query += " AND collection_id ILIKE ?"
        params.append(collection)
    if bodyPart:
        query += " AND BodyPartExamined ILIKE ?"
        params.append(bodyPart)
    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getModalityCounts(collection: str = "", bodyPart: str = "", format: str = ""):
    """
    Gets counts of Modality metadata.
    Allows filtering by collection and bodyPart.
    """
    client = get_client()
    query = "SELECT Modality, COUNT(DISTINCT SeriesInstanceUID) as count FROM index WHERE 1=1"
    params = []
    if collection:
        query += " AND collection_id ILIKE ?"
        params.append(collection)
    if bodyPart:
        query += " AND BodyPartExamined ILIKE ?"
        params.append(bodyPart)
    query += " GROUP BY Modality"
    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getPatient(collection: str = "", format: str = ""):
    """
    Gets Patient metadata.
    Allows filtering by collection.
    """
    client = get_client()
    query = "SELECT DISTINCT collection_id, PatientID, PatientSex, PatientAge FROM index WHERE 1=1"
    params = []
    if collection:
        query += " AND collection_id ILIKE ?"
        params.append(collection)
    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getPatientByCollectionAndModality(collection: str, modality: str, format: str = ""):
    """
    Requires specifying collection and modality.
    Gets Patient IDs.
    Returns a list of patient IDs.
    """
    client = get_client()
    query = "SELECT DISTINCT PatientID FROM index WHERE collection_id ILIKE ? AND Modality ILIKE ?"
    params = [collection, modality]
    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getStudy(collection: str, patientId: str = "", studyUid: str = "", format: str = ""):
    """
    Gets Study (visit/timepoint) metadata.
    Requires a collection parameter.
    Optional: patientId, studyUid, format
    """
    client = get_client()
    query = "SELECT DISTINCT collection_id, PatientID, StudyInstanceUID, StudyDate, StudyDescription FROM index WHERE collection_id ILIKE ?"
    params = [collection]
    if patientId:
        query += " AND PatientID ILIKE ?"
        params.append(patientId)
    if studyUid:
        query += " AND StudyInstanceUID ILIKE ?"
        params.append(studyUid)
    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getSeries(collection: str = "", patientId: str = "", studyUid: str = "", seriesUid: str = "",
              modality: str = "", bodyPart: str = "", manufacturer: str = "", manufacturerModel: str = "",
              format: str = ""):
    """
    Gets Series (scan) metadata.
    Allows filtering by collection, patient ID, study UID,
    series UID, modality, body part, manufacturer & model.
    """
    client = get_client()
    query = "SELECT * FROM index WHERE 1=1"
    params = []
    if collection:
        query += " AND collection_id ILIKE ?"
        params.append(collection)
    if patientId:
        query += " AND PatientID ILIKE ?"
        params.append(patientId)
    if studyUid:
        query += " AND StudyInstanceUID ILIKE ?"
        params.append(studyUid)
    if seriesUid:
        query += " AND SeriesInstanceUID ILIKE ?"
        params.append(seriesUid)
    if modality:
        query += " AND Modality ILIKE ?"
        params.append(modality)
    if bodyPart:
        query += " AND BodyPartExamined ILIKE ?"
        params.append(bodyPart)
    if manufacturer:
        query += " AND Manufacturer ILIKE ?"
        params.append(manufacturer)
    if manufacturerModel:
        query += " AND ManufacturerModelName ILIKE ?"
        params.append(manufacturerModel)

    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getSeriesList(uids: List[str], format: str = "df"):
    """
    Retrieve metadata for a list of series.

    Args:
        uids (List[str]): List of unique identifiers (series UIDs) to query.
        format (str, optional): Format of the output. Defaults to "df".

    Returns:
        Optional[pd.DataFrame]: A DataFrame containing the series metadata.
    """
    client = get_client()
    if not uids:
        return format_output(pd.DataFrame(), format=format)
    placeholders = ",".join(["?" for _ in uids])
    query = f"SELECT * FROM index WHERE SeriesInstanceUID IN ({placeholders})"
    df = client._duckdb_conn.execute(query, uids).df()
    return format_output(df, format=format)

def getSopInstanceUids(seriesUid: str, format: str = ""):
    """
    Gets SOP Instance UIDs from a specific series/scan.
    """
    client = get_client()
    try:
        urls = client.get_series_file_URLs(seriesUid)
        if not urls:
            return []

        first_url = urls[0]
        http_url = first_url.replace("s3://idc-open-data/", "https://idc-open-data.s3.amazonaws.com/")
        resp = requests.get(http_url, headers={'Range': 'bytes=0-1048575'}, proxies=get_proxy())
        if resp.status_code in [200, 206]:
            with io.BytesIO(resp.content) as f:
                ds = pydicom.dcmread(f, stop_before_pixels=True)
                filename_uid = os.path.basename(first_url).replace(".dcm", "")
                if ds.SOPInstanceUID != filename_uid:
                    _log.warning("SOPInstanceUID may not match file names. Returning instance UUIDs.")

                uids = [os.path.basename(url).replace(".dcm", "") for url in urls]
                if format == "df":
                    return pd.DataFrame(uids, columns=["SOPInstanceUID"])
                else:
                    return uids
    except Exception as e:
        _log.error(f"Error in getSopInstanceUids: {e}")
    return []

def getManufacturer(collection: str = "", modality: str = "", bodyPart: str = "", format: str = ""):
    """
    Gets manufacturer metadata.
    Allows filtering by collection, body part & modality.
    """
    client = get_client()
    query = "SELECT DISTINCT Manufacturer, ManufacturerModelName FROM index WHERE 1=1"
    params = []
    if collection:
        query += " AND collection_id ILIKE ?"
        params.append(collection)
    if modality:
        query += " AND Modality ILIKE ?"
        params.append(modality)
    if bodyPart:
        query += " AND BodyPartExamined ILIKE ?"
        params.append(bodyPart)
    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getManufacturerCounts(collection: str = "", modality: str = "", bodyPart: str = "", format: str = ""):
    """
    Gets counts of Manufacturer metadata.
    Allows filtering by collection, body part and modality.
    """
    client = get_client()
    query = "SELECT Manufacturer, COUNT(DISTINCT SeriesInstanceUID) as count FROM index WHERE 1=1"
    params = []
    if collection:
        query += " AND collection_id ILIKE ?"
        params.append(collection)
    if modality:
        query += " AND Modality ILIKE ?"
        params.append(modality)
    if bodyPart:
        query += " AND BodyPartExamined ILIKE ?"
        params.append(bodyPart)
    query += " GROUP BY Manufacturer"
    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def _processManifest(manifest_path: str) -> Union[List[str], str]:
    if manifest_path.endswith(".s5cmd"):
        return manifest_path

    try:
        with open(manifest_path, 'r', newline='') as f:
            first_line = f.readline().strip()
            f.seek(0)

            if first_line.startswith("downloadServerUrl="):
                _log.info("Detected NBIA manifest.")
                lines = f.readlines()
                uids = [line.strip() for line in lines[6:] if line.strip()]
                return uids

            if manifest_path.endswith((".csv", ".tsv")):
                delimiter = "," if manifest_path.endswith(".csv") else "\t"
                df = pd.read_csv(manifest_path, sep=delimiter)
                for col in ["SeriesInstanceUID", "SeriesUID"]:
                    if col in df.columns:
                        _log.info(f"Detected {col} in CSV/TSV manifest.")
                        return df[col].dropna().astype(str).tolist()

            _log.info("Treating manifest as one UID per line.")
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]

    except Exception as e:
        _log.error(f"Error processing manifest {manifest_path}: {e}")
        return []

def downloadSeries(series_data: Union[str, pd.DataFrame, List[str]],
                   number: int = 0,
                   path: str = "idcDownload",
                   input_type: str = "",
                   format: str = "",
                   max_workers: int = 10):
    """
    Ingests a set of seriesUids and downloads them.
    By default, series_data expects JSON containing "SeriesInstanceUID" elements.
    Set number = n to download the first n series if you don't want the full dataset.
    Saves to idcDownload folder in current directory if no path is specified.
    Set input_type = "list" to pass a list of Series UIDs instead of JSON.
    Set input_type = "df" to pass a dataframe that contains a "SeriesInstanceUID" column.
    Set input_type = "manifest" to pass the path of a TCIA, CSV/TSV or s5cmd manifest file.
    """
    client = get_client()
    series_uids = []
    s5cmd_manifest = None

    if input_type == "list":
        series_uids = series_data
    elif input_type == "df":
        series_uids = series_data['SeriesInstanceUID'].tolist()
    elif isinstance(series_data, str):
        processed = _processManifest(series_data)
        if isinstance(processed, str): # s5cmd path
            s5cmd_manifest = processed
        else:
            series_uids = processed
    else:
        series_uids = [item['SeriesInstanceUID'] for item in series_data]

    # Ensure the root directory exists
    try:
        if not os.path.exists(path):
            os.makedirs(path)
            _log.info(f"Directory '{path}' created successfully.")
        else:
            _log.info(f"Directory '{path}' already exists.")
    except OSError as e:
        _log.error(f"Failed to create directory '{path}': {e}")
        return None

    # Identify series to download vs. already existing
    existing_files = set(os.listdir(path))
    uids_to_download = []
    previously_downloaded_uids = []

    if not s5cmd_manifest:
        for seriesUID in series_uids:
            if seriesUID not in existing_files:
                uids_to_download.append(seriesUID)
            else:
                _log.warning(f"Series {seriesUID} already downloaded.")
                previously_downloaded_uids.append(seriesUID)

        # Apply 'number' limit if specified
        if number > 0:
            uids_to_download = uids_to_download[:number]

        _log.info(f"Found {len(previously_downloaded_uids)} previously downloaded series.")
        _log.info(f"Attempting to download {len(uids_to_download)} new series.")

    if s5cmd_manifest:
        _log.info(f"Downloading from s5cmd manifest {s5cmd_manifest} to {path}...")
        client.download_from_manifest(manifestFile=s5cmd_manifest, downloadDir=path)
    elif uids_to_download:
        _log.info(f"Downloading {len(uids_to_download)} series to {path}...")
        client.download_dicom_series(seriesInstanceUID=uids_to_download, downloadDir=path)
    elif not previously_downloaded_uids:
        _log.warning("No data found to download.")
        return None

    if format in ["df", "csv"]:
        all_uids = previously_downloaded_uids + uids_to_download
        if all_uids:
            return getSeriesList(all_uids, format=format)
    return None

def downloadImage(seriesUID: str, sopUID: str, path: str = "idcDownload"):
    """
    Downloads a DICOM image using the provided SeriesInstanceUID and SOPInstanceUID.
    """
    client = get_client()
    client.download_dicom_instance(sopInstanceUID=sopUID, downloadDir=path)

def getDicomTags(seriesUid: str, format: str = "df"):
    """
    Retrieves DICOM tag metadata for a given Series UID.
    """
    client = get_client()
    try:
        urls = client.get_series_file_URLs(seriesUid)
        if not urls:
            return None

        s3_url = urls[0]
        http_url = s3_url.replace("s3://idc-open-data/", "https://idc-open-data.s3.amazonaws.com/")

        _log.info(f"Fetching DICOM tags from {http_url}")
        resp = requests.get(http_url, headers={'Range': 'bytes=0-1048575'}, proxies=get_proxy())
        if resp.status_code in [200, 206]:
            with io.BytesIO(resp.content) as f:
                ds = pydicom.dcmread(f, stop_before_pixels=True)

                tag_data = []
                for element in ds:
                    if element.tag.group < 0x7fe0:
                        tag_data.append({
                            "element": f"({element.tag.group:04x},{element.tag.element:04x})",
                            "name": element.name,
                            "data": str(element.value)
                        })

                df = pd.DataFrame(tag_data)
                if format == "df":
                    return df
                else:
                    return tag_data
    except Exception as e:
        _log.error(f"Error in getDicomTags: {e}")
    return None

def getSegRefSeries(uid: str):
    """
    Gets DICOM tag metadata for a given SEG/RTSTRUCT series UID (scan)
    and looks up the corresponding original/reference series UID.
    """
    client = get_client()
    try:
        client.fetch_index('seg_index')
        client.sql_query("SELECT 1 FROM seg_index LIMIT 1")
        query = "SELECT segmented_SeriesInstanceUID FROM seg_index WHERE SeriesInstanceUID = ?"
        df = client._duckdb_conn.execute(query, [uid]).df()
        if not df.empty:
            return df.iloc[0]['segmented_SeriesInstanceUID']

        client.fetch_index('rtstruct_index')
        client.sql_query("SELECT 1 FROM rtstruct_index LIMIT 1")
        query = "SELECT referenced_SeriesInstanceUID FROM rtstruct_index WHERE SeriesInstanceUID = ?"
        df = client._duckdb_conn.execute(query, [uid]).df()
        if not df.empty:
            return df.iloc[0]['referenced_SeriesInstanceUID']
    except Exception as e:
        _log.error(f"Error in getSegRefSeries: {e}")

    _log.warning(f"Could not find reference series for {uid}")
    return "N/A"

def idcOhifViewer(data: Union[pd.DataFrame, List[dict]], max_rows: int = 500) -> Optional[HTML]:
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, pd.DataFrame):
        df = data.copy()
    else:
        return None

    if df.empty:
        return None

    if len(df) > max_rows:
        df = df.head(max_rows)

    base_url = "https://viewer.imaging.datacommons.cancer.gov/v3/viewer/"

    if 'SeriesInstanceUID' in df.columns and 'StudyInstanceUID' in df.columns:
        df['SeriesInstanceUID'] = df.apply(
            lambda row: f'<a href="{base_url}?StudyInstanceUIDs={row["StudyInstanceUID"]}&SeriesInstanceUIDs={row["SeriesInstanceUID"]}" target="_blank">{row["SeriesInstanceUID"]}</a>',
            axis=1
        )

    if 'StudyInstanceUID' in df.columns:
        df['StudyInstanceUID'] = df['StudyInstanceUID'].apply(
            lambda uid: f'<a href="{base_url}?StudyInstanceUIDs={uid}" target="_blank">{uid}</a>'
        )

    html_output = df.to_html(escape=False, index=False)
    return HTML(html_output)

def getCollectionDescriptions(format = ""):
    """
    Gets descriptions of collections and their DOIs.
    """
    client = get_client()
    try:
        client.fetch_index('collections_index')
        query = "SELECT * FROM collections_index"
        df = client.sql_query(query)
        return format_output(df, format=format)
    except Exception as e:
        _log.error(f"Error in getCollectionDescriptions: {e}")
    return None

def reportDataSummary(series_data, input_type="", report_type = "", format=""):
    """
    This function summarizes the input series_data by reporting
    on the various attributes like Collections, Modalities, etc.
    """
    uids = []
    if input_type == "list":
        uids = series_data
    elif input_type == "df":
        uids = series_data['SeriesInstanceUID'].tolist()
    elif isinstance(series_data, str):
        processed = _processManifest(series_data)
        if isinstance(processed, list):
            uids = processed
        else:
            _log.warning("Cannot generate report from s5cmd manifest path.")
            return None
    else:
        uids = [item['SeriesInstanceUID'] for item in series_data]

    df = getSeriesList(uids, format="df")

    if report_type == "doi":
        group = "DataDescriptionURI"
    else:
        group = "Collection"

    # Aggregation
    summary = df.groupby(group).agg({
        'Modality': 'unique',
        'LicenseName': 'unique',
        'Manufacturer': 'unique',
        'BodyPartExamined': 'unique',
        'PatientID': 'nunique',
        'StudyInstanceUID': 'nunique',
        'SeriesInstanceUID': 'nunique',
        'ImageCount': 'sum',
        'FileSize': 'sum'
    }).reset_index()

    summary.rename(columns={
        'PatientID': 'Subjects',
        'StudyInstanceUID': 'Studies',
        'SeriesInstanceUID': 'Series',
        'ImageCount': 'Images',
        'FileSize': 'File Size MB'
    }, inplace=True)

    summary['Disk Space'] = (summary['File Size MB'] * 1024 * 1024).apply(format_disk_space)

    if format == 'chart':
        for metric in ['Subjects', 'Studies', 'Series', 'Images']:
            fig = px.pie(summary, names=group, values=metric, title=f'{metric} Distribution')
            fig.show()

    if format == 'csv':
        summary.to_csv(f"idc_{report_type}_report.csv", index=False)

    return summary

def reportCollectionSummary(series_data, input_type="", format=""):
    """
    Generate a summary report about Collections from series metadata.
    """
    return reportDataSummary(series_data, input_type, report_type="collection", format=format)

def reportDoiSummary(series_data, input_type="", format=""):
    """
    Generate a summary report about DOIs from series metadata.
    """
    return reportDataSummary(series_data, input_type, report_type="doi", format=format)

def getSimpleSearch(
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
    format = ""):
    """
    All parameters are optional.
    Takes the same parameters as the SimpleSearch GUI.
    Use more parameters to narrow the number of subjects received.

    collections: list[str]   -- The DICOM collections of interest to you
    species: list[str]       -- Filter collections by species.
    modalities: list[str]    -- Filter collections by modality
    modalityAnded: bool      -- If true, only return subjects with all requested modalities, as opposed to any
    minStudies: int          -- The minimum number of studies a patient must have to be included in the results
    manufacturers: list[str] -- Imaging device manufacturers, e.g. SIEMENS
    bodyParts: list[str]     -- Body parts of interest, e.g. CHEST, ABDOMEN
    fromDate: str            -- First cutoff date, in YYYY/MM/DD format.
    toDate: str              -- Second cutoff date, in YYYY/MM/DD format.
    patients: list[str]      -- Patients to include in the output
    start: int               -- Start of returned series page. Defaults to 0.
    size: int                -- Size of returned series page. Defaults to 10.
    sortDirection            -- 'ascending' or 'descending'. Defaults to 'ascending'.
    sortField                -- 'subject', 'studies', 'series', or 'collection'. Defaults to 'subject'.
    format: str              -- Defaults to JSON. Can be set to "uids" to return a python list of
                                Series Instance UIDs or "manifest" to save a manifest file.
                                "manifest_text" can be used to return the manifest content as text.
    """
    client = get_client()

    query = "SELECT * FROM index"
    params = []
    where_clauses = []

    if collections:
        placeholders = ",".join(["?" for _ in collections])
        where_clauses.append(f"collection_id IN ({placeholders})")
        params.extend(collections)

    if species:
        client.fetch_index('collections_index')
        client.sql_query("SELECT 1 FROM collections_index LIMIT 1")
        placeholders = ",".join(["?" for _ in species])
        where_clauses.append(f"collection_id IN (SELECT collection_id FROM collections_index WHERE Species IN ({placeholders}))")
        params.extend([s.title() for s in species])

    if modalities:
        placeholders = ",".join(["?" for _ in modalities])
        where_clauses.append(f"Modality IN ({placeholders})")
        params.extend(modalities)

    if bodyParts:
        placeholders = ",".join(["?" for _ in bodyParts])
        where_clauses.append(f"BodyPartExamined IN ({placeholders})")
        params.extend(bodyParts)

    if manufacturers:
        placeholders = ",".join(["?" for _ in manufacturers])
        where_clauses.append(f"Manufacturer IN ({placeholders})")
        params.extend(manufacturers)

    if patients:
        placeholders = ",".join(["?" for _ in patients])
        where_clauses.append(f"PatientID IN ({placeholders})")
        params.extend(patients)

    if fromDate:
        isoFromDate = fromDate.replace("/", "-")
        where_clauses.append("StudyDate >= ?")
        params.append(isoFromDate)

    if toDate:
        isoToDate = toDate.replace("/", "-")
        where_clauses.append("StudyDate <= ?")
        params.append(isoToDate)

    if minStudies > 0:
        # Number of studies a patient has
        where_clauses.append("PatientID IN (SELECT PatientID FROM index GROUP BY PatientID HAVING COUNT(DISTINCT StudyInstanceUID) >= ?)")
        params.append(minStudies)

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    if modalityAnded and modalities:
        placeholders = ",".join(["?" for _ in modalities])
        query += f" AND PatientID IN (SELECT PatientID FROM index WHERE Modality IN ({placeholders}) GROUP BY PatientID HAVING COUNT(DISTINCT Modality) = ?)"
        params.extend(modalities)
        params.append(len(modalities))

    # Sorting
    sort_map = {
        'subject': 'PatientID',
        'studies': 'StudyInstanceUID',
        'series': 'SeriesInstanceUID',
        'collection': 'collection_id'
    }
    field = sort_map.get(sortField, 'PatientID')
    order = "ASC" if sortDirection == 'ascending' else "DESC"
    query += f" ORDER BY {field} {order}"

    if format not in ["uids", "manifest_text", "manifest"]:
        query += f" LIMIT {size} OFFSET {start}"

    df = client._duckdb_conn.execute(query, params).df()

    if format == "uids":
        return df['SeriesInstanceUID'].tolist()
    elif format == "manifest_text":
        return "\n".join(df['SeriesInstanceUID'].tolist())
    elif format == "manifest":
        uids = df['SeriesInstanceUID'].tolist()
        manifest_df = getSeriesList(uids, format="df")
        now = datetime.now()
        filename = now.strftime("manifest-%Y-%m-%d_%H-%M.csv")
        manifest_df.to_csv(filename, index=False)
        _log.info(f"Manifest saved as {filename}")
        return manifest_df

    return format_output(df, format=format)

# Unsupported function stubs with warnings

def setApiUrl(*args, **kwargs):
    warnings.warn("setApiUrl is not supported in the IDC module.", UserWarning)
    return None

def getSeriesSize(*args, **kwargs):
    warnings.warn("getSeriesSize is not supported in the IDC module.", UserWarning)
    return None

def getSharedCart(*args, **kwargs):
    warnings.warn("getSharedCart is not supported in the IDC module.", UserWarning)
    return None

def makeSeriesReport(*args, **kwargs):
    warnings.warn("makeSeriesReport is not supported in the IDC module.", UserWarning)
    return None

def reportSeriesSubmissionDate(*args, **kwargs):
    warnings.warn("reportSeriesSubmissionDate is not supported in the IDC module.", UserWarning)
    return None

def reportSeriesReleaseDate(*args, **kwargs):
    warnings.warn("reportSeriesReleaseDate is not supported in the IDC module.", UserWarning)
    return None

def getNewPatientsInCollection(*args, **kwargs):
    warnings.warn("getNewPatientsInCollection is not supported in the IDC module.", UserWarning)
    return None

def getNewStudiesInPatient(*args, **kwargs):
    warnings.warn("getNewStudiesInPatient is not supported in the IDC module.", UserWarning)
    return None

def getUpdatedSeries(*args, **kwargs):
    warnings.warn("getUpdatedSeries is not supported in the IDC module.", UserWarning)
    return None

def getDoiMetadata(*args, **kwargs):
    warnings.warn("getDoiMetadata is not supported in the IDC module.", UserWarning)
    return None

def getCollectionPatientCounts(*args, **kwargs):
    warnings.warn("getCollectionPatientCounts is not supported in the IDC module.", UserWarning)
    return None

def reportDicomTags(*args, **kwargs):
    warnings.warn("reportDicomTags is not supported in the IDC module.", UserWarning)
    return None
