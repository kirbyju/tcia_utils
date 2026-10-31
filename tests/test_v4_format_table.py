"""Display formatting keeps query values and list cells intact, across pandas versions."""

import warnings
import pytest
from tcia_utils import format_table


def test_display_missing_values_does_not_downcast_or_mutate_query_data():
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame(
        {
            "subjects": [12, None, 0],
            "python_supported": [True, False, None],
            "data_types": [["CT", "MR"], [], None],
            "cancer_types": [["Head, Neck"], ["Lung"], []],
        }
    )
    frame.attrs["tcia_provenance"] = {"release_fingerprint": "example"}
    original = frame.copy(deep=True)
    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        table = format_table(
            frame, columns=["subjects", "python_supported", "data_types", "cancer_types", "missing"]
        )
    assert table["subjects"].tolist() == [12, "Not reported", 0]
    assert table["python_supported"].tolist() == [True, False, "Not reported"]
    assert table["data_types"].tolist() == [["CT", "MR"], [], "Not reported"]
    assert table["cancer_types"].tolist() == [["Head, Neck"], ["Lung"], []]
    assert table["missing"].tolist() == ["Not reported"] * 3
    assert all(dtype == object for dtype in table.dtypes)
    pd.testing.assert_frame_equal(frame, original)
    assert table.attrs == frame.attrs
    table.attrs["tcia_provenance"]["release_fingerprint"] = "different"
    assert frame.attrs["tcia_provenance"]["release_fingerprint"] == "example"


def test_nullable_types_and_empty_table_are_supported_without_warnings():
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame(
        {
            "count": pd.Series([0, pd.NA], dtype="Int64"),
            "ready": pd.Series([False, pd.NA], dtype="boolean"),
            "label": pd.Series(["x", pd.NA], dtype="string"),
        }
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        table = format_table(frame, missing_label="Unknown")
        empty = format_table([], columns=["subjects", "studies"])
    assert table.loc[0, "count"] == 0 and table.loc[0, "ready"] == False
    assert table.loc[1].tolist() == ["Unknown"] * 3
    assert empty.empty and list(empty.columns) == ["subjects", "studies"]


def test_missing_label_must_be_text():
    with pytest.raises(TypeError, match="missing_label"):
        format_table([], missing_label=None)
