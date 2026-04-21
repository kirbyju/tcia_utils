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
    'series_size_MB': 'FileSize'
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

def getCollections(format: str = ""):
    client = get_client()
    collections = client.get_collections()
    df = pd.DataFrame(collections, columns=['collection_id'])
    return format_output(df, format=format)

def getBodyPart(collection: str = "", modality: str = "", format: str = ""):
    client = get_client()
    query = "SELECT DISTINCT BodyPartExamined FROM index WHERE 1=1"
    params = []
    if collection:
        query += " AND collection_id = ?"
        params.append(collection)
    if modality:
        query += " AND Modality = ?"
        params.append(modality)

    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getModality(collection: str = "", bodyPart: str = "", format: str = ""):
    client = get_client()
    query = "SELECT DISTINCT Modality FROM index WHERE 1=1"
    params = []
    if collection:
        query += " AND collection_id = ?"
        params.append(collection)
    if bodyPart:
        query += " AND BodyPartExamined = ?"
        params.append(bodyPart)
    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getPatient(collection: str = "", format: str = ""):
    client = get_client()
    query = "SELECT DISTINCT collection_id, PatientID, PatientSex, PatientAge FROM index WHERE 1=1"
    params = []
    if collection:
        query += " AND collection_id = ?"
        params.append(collection)
    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getPatientByCollectionAndModality(collection: str, modality: str, format: str = ""):
    client = get_client()
    query = "SELECT DISTINCT PatientID FROM index WHERE collection_id = ? AND Modality = ?"
    params = [collection, modality]
    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getStudy(collection: str, patientId: str = "", studyUid: str = "", format: str = ""):
    client = get_client()
    query = "SELECT DISTINCT collection_id, PatientID, StudyInstanceUID, StudyDate, StudyDescription FROM index WHERE collection_id = ?"
    params = [collection]
    if patientId:
        query += " AND PatientID = ?"
        params.append(patientId)
    if studyUid:
        query += " AND StudyInstanceUID = ?"
        params.append(studyUid)
    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getSeries(collection: str = "", patientId: str = "", studyUid: str = "", seriesUid: str = "",
              modality: str = "", bodyPart: str = "", manufacturer: str = "", manufacturerModel: str = "",
              format: str = ""):
    client = get_client()
    query = "SELECT * FROM index WHERE 1=1"
    params = []
    if collection:
        query += " AND collection_id = ?"
        params.append(collection)
    if patientId:
        query += " AND PatientID = ?"
        params.append(patientId)
    if studyUid:
        query += " AND StudyInstanceUID = ?"
        params.append(studyUid)
    if seriesUid:
        query += " AND SeriesInstanceUID = ?"
        params.append(seriesUid)
    if modality:
        query += " AND Modality = ?"
        params.append(modality)
    if bodyPart:
        query += " AND BodyPartExamined = ?"
        params.append(bodyPart)
    if manufacturer:
        query += " AND Manufacturer = ?"
        params.append(manufacturer)
    if manufacturerModel:
        query += " AND ManufacturerModelName = ?"
        params.append(manufacturerModel)

    df = client._duckdb_conn.execute(query, params).df()
    return format_output(df, format=format)

def getSeriesList(uids: List[str], format: str = "df"):
    client = get_client()
    if not uids:
        return format_output(pd.DataFrame(), format=format)
    placeholders = ",".join(["?" for _ in uids])
    query = f"SELECT * FROM index WHERE SeriesInstanceUID IN ({placeholders})"
    df = client._duckdb_conn.execute(query, uids).df()
    return format_output(df, format=format)

def getSopInstanceUids(seriesUid: str, format: str = ""):
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
    client = get_client()
    query = "SELECT DISTINCT Manufacturer, ManufacturerModelName FROM index WHERE 1=1"
    params = []
    if collection:
        query += " AND collection_id = ?"
        params.append(collection)
    if modality:
        query += " AND Modality = ?"
        params.append(modality)
    if bodyPart:
        query += " AND BodyPartExamined = ?"
        params.append(bodyPart)
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
    client = get_client()
    uids = []
    s5cmd_manifest = None

    if input_type == "list":
        uids = series_data
    elif input_type == "df":
        uids = series_data['SeriesInstanceUID'].tolist()
    elif isinstance(series_data, str):
        processed = _processManifest(series_data)
        if isinstance(processed, str): # s5cmd path
            s5cmd_manifest = processed
        else:
            uids = processed
    else:
        uids = [item['SeriesInstanceUID'] for item in series_data]

    if number > 0 and uids:
        uids = uids[:number]

    if s5cmd_manifest:
        _log.info(f"Downloading from s5cmd manifest {s5cmd_manifest} to {path}...")
        client.download_from_manifest(manifestFile=s5cmd_manifest, downloadDir=path)
    elif uids:
        _log.info(f"Downloading {len(uids)} series to {path}...")
        client.download_dicom_series(seriesInstanceUID=uids, downloadDir=path)
    else:
        _log.warning("No data found to download.")
        return None

    if format == "df" and uids:
        return getSeriesList(uids)
    return None

def downloadImage(seriesUID: str, sopUID: str, path: str = "idcDownload"):
    client = get_client()
    client.download_dicom_instance(sopInstanceUID=sopUID, downloadDir=path)

def getDicomTags(seriesUid: str, format: str = "df"):
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
    return reportDataSummary(series_data, input_type, report_type="collection", format=format)

def reportDoiSummary(series_data, input_type="", format=""):
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

def getModalityCounts(*args, **kwargs):
    warnings.warn("getModalityCounts is not supported in the IDC module.", UserWarning)
    return None

def getBodyPartCounts(*args, **kwargs):
    warnings.warn("getBodyPartCounts is not supported in the IDC module.", UserWarning)
    return None

def getManufacturerCounts(*args, **kwargs):
    warnings.warn("getManufacturerCounts is not supported in the IDC module.", UserWarning)
    return None

def reportDicomTags(*args, **kwargs):
    warnings.warn("reportDicomTags is not supported in the IDC module.", UserWarning)
    return None
