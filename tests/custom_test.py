import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from tcia_utils import nbia_v4 as nbia

# A public collection
SAMPLE_COLLECTION = "TCGA-KIRC"
# A sample shared cart
SAMPLE_SHARED_CART = "nbia-49121659384603347"

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

def test_reportDoiSummary(valid_series_uid):
    """
    Tests the reportDoiSummary function to ensure it returns a valid, non-null dataframe.
    This is an integration test that calls the live NBIA API.
    """
    # Get some series data from a public collection using getSeriesList
    series = nbia.getSeriesList(uids=[valid_series_uid])
    assert series is not None and not series.empty, "Failed to get series data for testing."

    # Generate the DOI summary report
    doi_summary = nbia.reportDoiSummary(series_data=series, input_type="df")

    # Assert that the result is a non-empty DataFrame
    assert isinstance(doi_summary, pd.DataFrame)
    assert not doi_summary.empty
    # Assert that the 'Identifier' column (from datacite) is present and not all null
    assert 'Identifier' in doi_summary.columns
    assert not doi_summary['Identifier'].isnull().all()


@patch('tcia_utils.nbia_v4.requests.get')
def test_getSharedCart_uses_v1_api(mock_get):
    """
    Tests that getSharedCart correctly calls the v1 API endpoint.
    It uses a mock to inspect the URL being called without making a real network request.
    """
    # Configure the mock to return a dummy response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"some_key": "some_value"}]
    # Make sure content is not empty
    mock_response.content = b'[{"some_key": "some_value"}]'
    mock_get.return_value = mock_response

    # Call getSharedCart
    nbia.getSharedCart(name=SAMPLE_SHARED_CART, format="df")

    # Assert that requests.get was called
    mock_get.assert_called_once()

    # Get the URL from the mock call
    called_url = mock_get.call_args[0][0]

    # Assert that the URL contains the v1 API path
    assert "services/v1/getContentsByName" in called_url
