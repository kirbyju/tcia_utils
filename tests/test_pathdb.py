import os
import pandas as pd
import pytest
from tcia_utils import pathdb

# Sample data mimicking the output of pathdb.getImages()
SAMPLE_IMAGES_DATA = [
    {
        "collectionName": "TCGA-LUAD",
        "collectionId": 123,
        "subjectId": "TCGA-05-4244",
        "imageId": "TCGA-05-4244-01A-01-TS1",
        "imageUrl": "https://pathdb.cancerimagingarchive.net/media/images/TCGA-05-4244-01A-01-TS1.svs"
    },
    {
        "collectionName": "TCGA-LUAD",
        "collectionId": 123,
        "subjectId": "TCGA-05-4245",
        "imageId": "TCGA-05-4245-01A-01-TS1",
        "imageUrl": "https://pathdb.cancerimagingarchive.net/media/images/TCGA-05-4245-01A-01-TS1.svs"
    },
    {
        "collectionName": "TCGA-BRCA",
        "collectionId": 456,
        "subjectId": "TCGA-A1-A0SP",
        "imageId": "TCGA-A1-A0SP-01Z-00-DX1",
        "imageUrl": "https://pathdb.cancerimagingarchive.net/media/images/TCGA-A1-A0SP-01Z-00-DX1.svs"
    },
    # Image with no URL to test error handling
    {
        "collectionName": "TCGA-BRCA",
        "collectionId": 456,
        "subjectId": "TCGA-A1-A0SQ",
        "imageId": "TCGA-A1-A0SQ-01Z-00-DX1",
        "imageUrl": ""
    }
]

@pytest.fixture
def mock_requests_get(mocker):
    """Fixture to mock requests.get."""
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    # Use bytes content for iter_content
    mock_response.iter_content.return_value = [b'fake-image-data-chunk-1', b'fake-image-data-chunk-2']
    mock_response.raise_for_status = mocker.Mock()
    return mocker.patch('requests.get', return_value=mock_response)

def test_downloadImages_basic(tmp_path, mock_requests_get):
    """Test basic functionality of downloadImages with a list."""
    download_path = tmp_path / "downloads"

    # The function to test
    pathdb.downloadImages(SAMPLE_IMAGES_DATA, path=str(download_path))

    # Assertions
    assert download_path.exists()

    # Check that 3 files were "downloaded" (one has no URL)
    downloaded_files = os.listdir(download_path)
    assert len(downloaded_files) == 3
    assert "TCGA-05-4244-01A-01-TS1.svs" in downloaded_files
    assert "TCGA-05-4245-01A-01-TS1.svs" in downloaded_files
    assert "TCGA-A1-A0SP-01Z-00-DX1.svs" in downloaded_files

    # Check that requests.get was called for the 3 valid URLs
    assert mock_requests_get.call_count == 3

    # Check content of one file
    with open(download_path / "TCGA-05-4244-01A-01-TS1.svs", "rb") as f:
        content = f.read()
        assert content == b'fake-image-data-chunk-1fake-image-data-chunk-2'

def test_downloadImages_with_existing_files(tmp_path, mock_requests_get):
    """Test that existing files are skipped."""
    download_path = tmp_path / "downloads"
    download_path.mkdir()

    # Create a dummy file to simulate an existing download
    existing_filename = "TCGA-05-4244-01A-01-TS1.svs"
    (download_path / existing_filename).touch()

    pathdb.downloadImages(SAMPLE_IMAGES_DATA, path=str(download_path))

    # Check that only the 2 new files were downloaded
    # requests.get should only be called for the non-existing files
    assert mock_requests_get.call_count == 2

    # The directory should contain 3 files in total
    downloaded_files = os.listdir(download_path)
    assert len(downloaded_files) == 3

def test_downloadImages_with_dataframe(tmp_path, mock_requests_get):
    """Test functionality with pandas DataFrame as input."""
    download_path = tmp_path / "downloads"
    images_df = pd.DataFrame(SAMPLE_IMAGES_DATA)

    pathdb.downloadImages(images_df, path=str(download_path))

    assert download_path.exists()
    downloaded_files = os.listdir(download_path)
    assert len(downloaded_files) == 3
    assert "TCGA-05-4244-01A-01-TS1.svs" in downloaded_files

def test_downloadImages_empty_list(tmp_path, mock_requests_get):
    """Test that it handles an empty list gracefully."""
    download_path = tmp_path / "downloads"

    pathdb.downloadImages([], path=str(download_path))

    # Should not create the directory if there's nothing to download
    assert not download_path.exists()
    # Should not make any network calls
    assert mock_requests_get.call_count == 0
