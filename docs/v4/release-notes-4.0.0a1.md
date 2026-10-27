# TCIA Utils 4.0.0a1 — community testing preview

This first alpha introduces a dataset- and participant-oriented `TCIAClient`
over TCIA's shared REST service. It is an opt-in prerelease: ordinary pip
installations continue selecting the stable 3.x release.

```bash
python -m pip install "tcia_utils[tables]==4.0.0a1"
```

Requires Python 3.11 or newer. The core dependency is httpx; pandas tables,
notebook tools, and imaging helpers are optional extras. This API does not
preserve the old provider-specific 3.x imports. Keep existing scripts in a
separate 3.x environment and consult [MIGRATION.md](../../MIGRATION.md).

## Try the workshop

[Open in Colab](https://colab.research.google.com/github/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/TCIA_Dataset_and_Participant_Queries.ipynb)
or [view/download the notebook](https://github.com/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/TCIA_Dataset_and_Participant_Queries.ipynb).
It installs this exact alpha and covers CMB-GEC dataset discovery/download
planning, cross-collection modality/diagnosis cohorts, public brain MR/NIfTI
segmentations, and a controlled facial-CT/SEG selection. Public metadata queries
run by default; imaging transfers require explicit opt-in.

## Known limits

- The CMB-GEC public tissue-slide package requires its collection download link;
  the Data Retriever CLI cannot retrieve that group yet.
- Indexed NIfTI assets do not have a per-file API download route. Use published
  package links. Geometry status is available, but detailed geometry fields
  were empty for inspected files; the notebook includes an optional local check.
- Clinical coverage is incomplete and includes explicitly marked inferences.
  Original source files remain authoritative. Participant identities remain
  dataset-scoped; cross-dataset person deduplication is not asserted.
- Co-occurring CT and SEG metadata does not verify exact DICOM source references.
  Review those references and label definitions after an authorized download.
- Public controlled-file metadata and manifests do not confer access approval.
- Local notebook execution is verified; hosted Colab remains a community test.

## Feedback

Report problems at https://github.com/kirbyju/tcia_utils/issues with the failing
cell or method, Python/package versions, and error text. Include a small public
example when possible. Never include an API key, credential file, private data,
or a controlled image payload.
