
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import os
import shutil
from tcia_utils import idc

class TestIDCDownload(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_tcia_download"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    @patch("tcia_utils.idc.get_client")
    @patch("tcia_utils.idc.getSeriesList")
    def test_downloadSeries_flat_already_exists(self, mock_getSeriesList, mock_get_client):
        # Setup mock client
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        series_uids = ["uid1", "uid2"]
        # Create directory for uid1 to simulate already downloaded
        os.makedirs(os.path.join(self.test_dir, "uid1"))
        with open(os.path.join(self.test_dir, "uid1", "test.dcm"), "w") as f:
            f.write("dummy")

        idc.downloadSeries(series_uids, path=self.test_dir, input_type="list", template="flat")

        # Should only attempt to download uid2
        mock_client.download_dicom_series.assert_called_once()
        args, kwargs = mock_client.download_dicom_series.call_args
        self.assertEqual(kwargs['seriesInstanceUID'], ["uid2"])
        self.assertEqual(kwargs['dirTemplate'], "%SeriesInstanceUID")
        self.assertEqual(kwargs['use_s5cmd_sync'], True)

    @patch("tcia_utils.idc.get_client")
    @patch("tcia_utils.idc.getSeriesList")
    def test_downloadSeries_nested_already_exists(self, mock_getSeriesList, mock_get_client):
        # Setup mock client
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        series_uids = ["uid1", "uid2"]

        # Mock metadata for both series
        mock_metadata = pd.DataFrame([
            {
                "SeriesInstanceUID": "uid1",
                "Collection": "coll1",
                "PatientID": "pat1",
                "StudyInstanceUID": "study1",
                "Modality": "CT"
            },
            {
                "SeriesInstanceUID": "uid2",
                "Collection": "coll2",
                "PatientID": "pat2",
                "StudyInstanceUID": "study2",
                "Modality": "MR"
            }
        ])
        mock_getSeriesList.return_value = mock_metadata

        # Create nested directory for uid1
        # Default nested: %collection_id/%PatientID/%StudyInstanceUID/%Modality_%SeriesInstanceUID
        uid1_path = os.path.join(self.test_dir, "coll1", "pat1", "study1", "CT_uid1")
        os.makedirs(uid1_path)
        with open(os.path.join(uid1_path, "test.dcm"), "w") as f:
            f.write("dummy")

        idc.downloadSeries(series_uids, path=self.test_dir, input_type="list", template="nested")

        # Should only attempt to download uid2
        mock_client.download_dicom_series.assert_called_once()
        args, kwargs = mock_client.download_dicom_series.call_args
        self.assertEqual(kwargs['seriesInstanceUID'], ["uid2"])
        self.assertEqual(kwargs['dirTemplate'], "%collection_id/%PatientID/%StudyInstanceUID/%Modality_%SeriesInstanceUID")

    @patch("tcia_utils.idc.get_client")
    @patch("tcia_utils.idc.getSeriesList")
    def test_downloadSeries_number_limit(self, mock_getSeriesList, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        series_uids = ["uid1", "uid2", "uid3"]
        # uid1 exists
        os.makedirs(os.path.join(self.test_dir, "uid1"))
        with open(os.path.join(self.test_dir, "uid1", "test.dcm"), "w") as f:
            f.write("dummy")

        # Limit to 1 NEW series
        idc.downloadSeries(series_uids, path=self.test_dir, input_type="list", number=1, template="flat")

        mock_client.download_dicom_series.assert_called_once()
        args, kwargs = mock_client.download_dicom_series.call_args
        self.assertEqual(kwargs['seriesInstanceUID'], ["uid2"])

    @patch("tcia_utils.idc.get_client")
    @patch("tcia_utils.idc._processManifest")
    def test_downloadSeries_s5cmd_manifest(self, mock_processManifest, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_processManifest.return_value = "test.s5cmd"

        idc.downloadSeries("test.s5cmd", path=self.test_dir, input_type="manifest", template="nested")

        mock_client.download_from_manifest.assert_called_once()
        args, kwargs = mock_client.download_from_manifest.call_args
        self.assertEqual(kwargs['manifestFile'], "test.s5cmd")
        self.assertEqual(kwargs['dirTemplate'], "%collection_id/%PatientID/%StudyInstanceUID/%Modality_%SeriesInstanceUID")
        self.assertEqual(kwargs['use_s5cmd_sync'], True)

if __name__ == "__main__":
    unittest.main()
