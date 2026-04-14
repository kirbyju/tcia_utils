import unittest
import pandas as pd
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
        # We need a known SEG or RTSTRUCT series.
        # From previous exploration: rider_pilot has an SR with UID
        # 1.2.276.0.7230010.3.1.3.1031255639.2476.1345992261.66
        # Let's find an RTSTRUCT in 4d_lung
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

if __name__ == '__main__':
    unittest.main()
