import unittest
import pandas as pd
import os
from tcia_utils import idc

class TestIDC(unittest.TestCase):
    def test_getCollections(self):
        collections = idc.getCollections(format="df")
        self.assertIsInstance(collections, pd.DataFrame)
        self.assertIn("Collection", collections.columns)
        self.assertGreater(len(collections), 0)

    def test_getSeries(self):
        series = idc.getSeries(collection="rider_pilot", format="df")
        self.assertIsInstance(series, pd.DataFrame)
        self.assertIn("SeriesInstanceUID", series.columns)
        self.assertIn("Collection", series.columns)
        self.assertEqual(series["Collection"].iloc[0], "rider_pilot")

    def test_getPatient(self):
        patients = idc.getPatient(collection="rider_pilot", format="df")
        self.assertIsInstance(patients, pd.DataFrame)
        self.assertIn("PatientID", patients.columns)
        self.assertGreater(len(patients), 0)

    def test_getModality(self):
        modalities = idc.getModality(collection="rider_pilot", format="df")
        self.assertIsInstance(modalities, pd.DataFrame)
        self.assertIn("Modality", modalities.columns)

    def test_getBodyPart(self):
        body_parts = idc.getBodyPart(collection="rider_pilot", format="df")
        self.assertIsInstance(body_parts, pd.DataFrame)
        self.assertIn("BodyPartExamined", body_parts.columns)

    def test_getSeriesList(self):
        # First get some UIDs
        series = idc.getSeries(collection="rider_pilot", format="json")
        uids = [s["SeriesInstanceUID"] for s in series[:2]]
        series_list = idc.getSeriesList(uids, format="df")
        self.assertEqual(len(series_list), 2)
        self.assertIn(uids[0], series_list["SeriesInstanceUID"].values)

    def test_getSegRefSeries(self):
        series = idc.getSeries(collection="4d_lung", modality="RTSTRUCT", format="json")
        if series:
            uid = series[0]["SeriesInstanceUID"]
            ref_uid = idc.getSegRefSeries(uid)
            self.assertNotEqual(ref_uid, "N/A")
            self.assertTrue(ref_uid.startswith("1."))

    def test_reportCollectionSummary(self):
        series = idc.getSeries(collection="rider_pilot", format="json")
        summary = idc.reportCollectionSummary(series[:5])
        self.assertIsInstance(summary, pd.DataFrame)
        self.assertIn("Collection", summary.columns)
        self.assertIn("Subjects", summary.columns)

    def test_getSimpleSearch(self):
        # Test basic search
        results = idc.getSimpleSearch(collections=["rider_pilot"], modalities=["CT"], format="df")
        self.assertIsInstance(results, pd.DataFrame)
        self.assertGreater(len(results), 0)
        self.assertTrue(all(results["Collection"] == "rider_pilot"))

        # Test uids format
        uids = idc.getSimpleSearch(collections=["rider_pilot"], format="uids")
        self.assertIsInstance(uids, list)
        self.assertGreater(len(uids), 0)
        self.assertIsInstance(uids[0], str)

    def test_manifest_nbia(self):
        content = "downloadServerUrl=https://public.cancerimagingarchive.net/nbia-download/servlet/DownloadServlet\nline2\nline3\nline4\nline5\nline6\nUID1\nUID2"
        with open("test_nbia.tcia", "w") as f:
            f.write(content)
        uids = idc._processManifest("test_nbia.tcia")
        self.assertEqual(uids, ["UID1", "UID2"])
        os.remove("test_nbia.tcia")

    def test_manifest_csv(self):
        df = pd.DataFrame({"SeriesInstanceUID": ["UID1", "UID2"], "Other": [1, 2]})
        df.to_csv("test.csv", index=False)
        uids = idc._processManifest("test.csv")
        self.assertEqual(uids, ["UID1", "UID2"])
        os.remove("test.csv")

    def test_manifest_s5cmd(self):
        manifest_path = "test.s5cmd"
        with open(manifest_path, "w") as f:
            f.write("cp s3://bucket/obj ./dest")
        processed = idc._processManifest(manifest_path)
        self.assertEqual(processed, manifest_path)
        os.remove(manifest_path)

if __name__ == '__main__':
    unittest.main()
