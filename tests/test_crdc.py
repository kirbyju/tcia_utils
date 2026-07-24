import pytest
import pandas as pd
import json
from unittest.mock import patch, MagicMock
from pathlib import Path
from tcia_utils import crdc

@pytest.fixture
def mock_graphql_type():
    return {
        "__type": {
            "fields": [
                {"name": "participantsCount"},
                {"name": "samplesCount"},
                {"name": "filesCount"},
                {"name": "diagnosesCount"},
                {"name": "treatmentsCount"},
                {"name": "imagesCount"}
            ]
        }
    }

def test_gen3_auth_key():
    auth = crdc.Gen3Auth(api_key="my-key-123")
    assert auth.api_key == "my-key-123"

def test_gen3_auth_file(tmp_path):
    cred_file = tmp_path / "key.json"
    cred_file.write_text(json.dumps({"api_key": "my-key-file"}))
    auth = crdc.Gen3Auth(credentials_file=cred_file)
    assert auth.api_key == "my-key-file"

@patch("urllib.request.urlopen")
def test_gen3_auth_get_access_token(mock_urlopen):
    # Mocking standard post
    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps({"access_token": "temp-token-xyz"}).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    auth = crdc.Gen3Auth(api_key="test-api-key")
    token = auth.get_access_token("some-commons.io")
    assert token == "temp-token-xyz"
    assert auth.tokens["some-commons.io"] == "temp-token-xyz"

    # Verify cached retrieval
    token_cached = auth.get_access_token("some-commons.io")
    assert token_cached == "temp-token-xyz"
    # urlopen should only have been called once
    mock_urlopen.assert_called_once()

@patch("urllib.request.urlopen")
def test_query_graphql(mock_urlopen):
    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps({"data": {"studies": [{"study_name": "Study A"}]}}).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    res = crdc.query_graphql("query { studies { study_name } }")
    assert res == {"studies": [{"study_name": "Study A"}]}

@patch("urllib.request.urlopen")
def test_get_studies(mock_urlopen):
    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps({"data": {"studies": [{"study_name": "Study A", "phs_accession": "phs004225"}]}}).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    res = crdc.get_studies()
    assert len(res) == 1
    assert res[0]["study_name"] == "Study A"

@patch("urllib.request.urlopen")
def test_get_counts(mock_urlopen, mock_graphql_type):
    # GraphQL returns schema fields first, then counts
    mock_response_intro = MagicMock()
    mock_response_intro.read.return_value = json.dumps({"data": mock_graphql_type}).encode("utf-8")

    mock_response_counts = MagicMock()
    mock_response_counts.read.return_value = json.dumps({"data": {"participantsCount": 100, "filesCount": 500}}).encode("utf-8")

    mock_urlopen.return_value.__enter__.side_effect = [mock_response_intro, mock_response_counts]

    counts = crdc.get_counts()
    assert counts == {"participantsCount": 100, "filesCount": 500}

@patch("urllib.request.urlopen")
def test_get_file_overview(mock_urlopen):
    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps({
        "data": {
            "fileOverview": [
                {"file_name": "img1.dcm", "file_type": "DICOM", "file_id": "file-123"}
            ]
        }
    }).encode("utf-8")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    res = crdc.get_file_overview(studies=["TCGA-GBM"])
    assert len(res) == 1
    assert res[0]["file_name"] == "img1.dcm"

@patch("urllib.request.urlopen")
def test_download_file_by_drs(mock_urlopen, tmp_path):
    # Mock Gen3 signed url response, then the file data download
    mock_signed_response = MagicMock()
    mock_signed_response.read.return_value = json.dumps({"url": "https://signed-url.com/img1.dcm"}).encode("utf-8")

    mock_file_response = MagicMock()
    mock_file_response.read.return_value = b"DICOM payload data"

    mock_urlopen.return_value.__enter__.side_effect = [mock_signed_response, mock_file_response]

    auth = crdc.Gen3Auth(api_key="test-api-key")
    # Mock token fetch
    auth.tokens["nci-crdc.datacommons.io"] = "some-token"

    dest = crdc.download_file_by_drs(
        drs_uri="drs://nci-crdc.datacommons.io/dg.4DFC/d15fed7c-2edf-5e3a-bc24-64c7173ae614",
        output_dir=tmp_path,
        auth=auth,
        file_name="img1.dcm"
    )

    assert dest.exists()
    assert dest.read_bytes() == b"DICOM payload data"

def test_parse_manifest(tmp_path):
    # Write a test CSV manifest
    csv_file = tmp_path / "manifest.csv"
    df = pd.DataFrame({
        "PatientID": ["p1", "p2"],
        "drs_uri": ["drs://nci-crdc.datacommons.io/1", "drs://nci-crdc.datacommons.io/2"]
    })
    df.to_csv(csv_file, index=False)

    res = crdc.parse_manifest(csv_file)
    assert res == ["drs://nci-crdc.datacommons.io/1", "drs://nci-crdc.datacommons.io/2"]

@patch("tcia_utils.crdc.download_file_by_drs")
def test_download_data_list(mock_download_file_by_drs, tmp_path):
    mock_download_file_by_drs.return_value = tmp_path / "img1.dcm"

    drs_uris = ["drs://nci-crdc.datacommons.io/1", "dg.4DFC/2"]
    paths = crdc.download_data(drs_uris, output_dir=tmp_path)

    assert len(paths) == 2
    assert mock_download_file_by_drs.call_count == 2
