import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import importlib
nbia = importlib.import_module("tcia_utils.nbia-v4")
import os

# A public collection
SAMPLE_COLLECTION = "TCGA-KIRC"
# A public patient ID from that collection
SAMPLE_PATIENT_ID = "TCGA-BP-4170"

@pytest.fixture(scope="module")
def valid_series_uid():
    """
    Fixture to get a valid SeriesInstanceUID from a public collection.
    This runs once per module and makes the UID available to other tests.
    """
    series = nbia.getSeries(collection=SAMPLE_COLLECTION, format="df")
    if series is not None and not series.empty:
        return series['SeriesInstanceUID'].iloc[0]
    else:
        pytest.skip("Could not retrieve series list to get a valid UID.")

def test_manifestToList(tmp_path):
    """
    Tests the manifestToList function to ensure it correctly parses a manifest file,
    removes the header, and returns a list of SeriesInstanceUIDs.
    """
    # Create a dummy manifest file with a realistic header
    manifest_content = """\
#Number of Series: 2
#UID,Description,Subject ID,Study UID,Study Date,Series Date,Collection,Series UID
#
#
#
1.2.3.4.5
6.7.8.9.10
"""
    manifest_file = tmp_path / "test_manifest.tcia"
    # A realistic manifest file needs a proper header for the check to pass
    manifest_content_with_header = "downloadServerUrl=https://services.cancerimagingarchive.net/nbia-api/services/v1/getImage\n" + manifest_content
    manifest_file.write_text(manifest_content_with_header)

    # Call the function with the path to the dummy manifest file
    result = nbia.manifestToList(str(manifest_file))

    # Assert that the function returns the expected list of UIDs
    expected_uids = ["1.2.3.4.5", "6.7.8.9.10"]
    assert result == expected_uids


def test_formatSeriesInput_df():
    """
    Tests the formatSeriesInput function with a DataFrame input.
    Ensures that the columns are correctly renamed and formatted.
    """
    # Create a sample DataFrame
    data = {
        'Series ID': ['1.2.3', '4.5.6'],
        'Subject ID': ['p1', 'p2'],
        'Study UID': ['s1', 's2'],
        'Study Date': ['2023-01-01', '2023-01-02'],
        'Date Released': ['2023-01-01T10:00:00', '2023-01-02T10:00:00'],
        'TimeStamp': ['2023-01-01T12:00:00', '2023-01-02T12:00:00']
    }
    input_df = pd.DataFrame(data)

    # Call the function
    formatted_df = nbia.formatSeriesInput(series_data=input_df, input_type="df", api_url="")

    # Assertions
    assert 'SeriesInstanceUID' in formatted_df.columns
    assert 'PatientID' in formatted_df.columns
    assert 'StudyInstanceUID' in formatted_df.columns
    assert pd.api.types.is_datetime64_any_dtype(formatted_df['DateReleased'])
    assert pd.api.types.is_datetime64_any_dtype(formatted_df['TimeStamp'])
    assert formatted_df['SeriesInstanceUID'].tolist() == ['1.2.3', '4.5.6']


def test_viewSeries_raises_error():
    """
    Tests that viewSeries raises a NotImplementedError as expected.
    """
    with pytest.raises(NotImplementedError):
        nbia.viewSeries()


def test_viewSeriesAnnotation_raises_error():
    """
    Tests that viewSeriesAnnotation raises a NotImplementedError as expected.
    """
    with pytest.raises(NotImplementedError):
        nbia.viewSeriesAnnotation()


def test_getCollections():
    """
    Tests the getCollections function to ensure it returns a list of collections.
    This is an integration test that calls the live NBIA API.
    """
    collections = nbia.getCollections(format="df")
    assert isinstance(collections, pd.DataFrame)
    assert 'Collection' in collections.columns
    assert len(collections) > 0


def test_getSeries():
    """
    Tests the getSeries function with a specific public collection.
    This is an integration test.
    """
    series = nbia.getSeries(collection=SAMPLE_COLLECTION, format="df")
    assert isinstance(series, pd.DataFrame)
    assert 'SeriesInstanceUID' in series.columns
    assert len(series) > 0


def test_getSeriesList(valid_series_uid):
    """
    Tests the getSeriesList function with a specific public series UID.
    This is an integration test.
    """
    metadata = nbia.getSeriesList(uids=[valid_series_uid])
    assert isinstance(metadata, pd.DataFrame)
    assert 'SeriesInstanceUID' in metadata.columns
    assert metadata.loc[0, 'SeriesInstanceUID'] == valid_series_uid


@patch('requests.get')
def test_queryData_404_error(mock_get, caplog):
    """
    Tests the queryData function's handling of a 404 Not Found error.
    Uses mocking to simulate an HTTP 404 response.
    """
    # Configure the mock to raise an HTTPError for 404 status
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = nbia.requests.exceptions.HTTPError(response=mock_response)

    mock_get.return_value = mock_response

    # Call the function that is expected to fail
    result = nbia.queryData(endpoint="getSeries", options={'SeriesInstanceUID': 'invalid_uid'}, api_url="")

    # Assertions
    assert result is None
    assert "Resource Not Found" in caplog.text


@patch('requests.get')
def test_queryData_connection_error(mock_get, caplog):
    """
    Tests the queryData function's handling of a connection error.
    """
    # Configure the mock to raise a ConnectionError
    mock_get.side_effect = nbia.requests.exceptions.ConnectionError

    # Call the function
    result = nbia.queryData(endpoint="getSeries", options={}, api_url="")

    # Assertions
    assert result is None
    assert "Connection Error" in caplog.text


def test_makeCredentialFile(tmp_path):
    """
    Tests the makeCredentialFile function to ensure it creates a credential file
    with the correct content in a specified directory.
    """
    # Change the current working directory to the temporary directory
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Define user and password
        user = "testuser"
        pw = "testpassword"

        # Call the function
        nbia.makeCredentialFile(user=user, pw=pw)

        # Check if the file was created and has the correct content
        cred_file = tmp_path / "credentials.txt"
        assert cred_file.exists()
        with open(cred_file, 'r') as f:
            content = f.read()
            assert f"userName={user}" in content
            assert f"passWord={pw}" in content
    finally:
        # Change back to the original working directory
        os.chdir(original_cwd)


def test_downloadSeries(tmp_path, valid_series_uid):
    """
    Tests the downloadSeries function by downloading a single series.
    This is an integration test and may take a moment to run.
    """
    # Use a list with a single, known public series UID from the fixture
    series_to_download = [valid_series_uid]

    # Call the download function
    result_df = nbia.downloadSeries(
        series_data=series_to_download,
        input_type='list',
        path=str(tmp_path),
        format='df'
    )

    # Assertions
    # Check that a DataFrame is returned with metadata
    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty
    assert result_df.loc[0, 'SeriesInstanceUID'] == valid_series_uid

    # Check that the series directory was created
    series_dir = tmp_path / valid_series_uid
    assert series_dir.exists()
    assert series_dir.is_dir()

    # Check that at least one DICOM file was downloaded
    downloaded_files = list(series_dir.glob('*.dcm'))
    assert len(downloaded_files) > 0
