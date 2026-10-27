**2026-10-25 replacement workshop:** the user requested one conversational,
standalone notebook for junior data scientists. The five drafts described below
are superseded by `TCIA_Dataset_and_Participant_Queries.ipynb`, pinned through
`examples/notebooks.json` to TCIA_Notebooks commit
`1214330acee1914b81cdc3ba4e8272bf916fd29a`.

The replacement passed live metadata execution and offline replay using the
installed 4.0.0a1 wheel on Python 3.13.7. Nine focused tests passed. Its default
CT/diagnosis cohort contains 17 dataset-scoped participant records across CMB-GEC
and CMB-LCA; the brain MR query links 2,261 NIfTI segmentation files to 1,106
participant records. The controlled example identifies 137 CT/SEG participant-
study pairs and exports a two-file subset for one of them. These are metadata
observations, not deduplicated people or verified source-series references.

The observed release fingerprint was
`171f50880bb668eae21dd505a8de71bb68e4bd4a840966c378528c762a70fbe2`.
The notebook has no learner-side helper or fixture dependency. A maintainer-only
cassette replays recorded public metadata, with all transfer subprocesses blocked.
No image payloads or credentials were used; hosted Colab was not tested. The
CMB-GEC tissue-slide group still needs its collection download link, and the
NIfTI API does not expose per-file transfers or populated detailed geometry for
the inspected files. The canonical notebook repository contains the dated live,
replay, and download-identifier coverage reports.

---

# Notebook validation

**2026-10-15 post-deployment update:** all five live metadata notebooks passed again against the installed `4.0.0a1` wheel, with zero unavailable-route messages. The [current live report](notebook-live-validation-2026-10-15.json) supersedes the hosted failure observations below. No imaging payloads were transferred.

## Original 2026-10-14 validation

The five canonical notebooks were executed from the built `4.0.0a1` wheel in an isolated Python 3.13 examples environment. The environment used `idc-index` 0.13.0, IDC index data 25.0.0 (reported by IDC as `v25`), and pandas 3.0.6. The [fixture report](notebook-fixture-validation.json) records five successful synthetic runs. Fixture mode uses an in-memory HTTP transport, labels its output **SYNTHETIC FIXTURE MODE**, and re-raises client errors so a changed API cannot silently pass.

The separate [live report](notebook-live-validation.json) records five completed, bounded metadata runs against `https://tcia.duckdns.org`, whose observed metadata release fingerprint was `a74b80068740d823657379be97e704e6db5229424a731de47397aa534213ff29`. Notebook 02 verified the visible TCIA `4D-Lung` DOI against the local IDC index, queried up to three series ordered by UID, printed the source DOI and license label, obtained a viewer URL, and exported a local metadata-only series manifest. Notebook 04 wrote a local controlled-file DRS manifest and accounting companion; it did not run Data Retriever or request a payload.

The live participant-assets, clinical-subjects, and clinical-facts calls returned HTTP 500 and were labeled unavailable in notebook 05. Notebook 03's bounded `4D-Lung` public-asset query did not report an error, but that does not supersede the separate nonempty public-asset route failure in the [client smoke report](client-live-smoke.json). The observed host is not yet validated for those four routes. The service [compatibility review](service-compatibility.md) describes the required post-deployment probes.

Notebook structure validation passed for all five files, and the runner rejected emitted private paths, credential-shaped text, and notebook error outputs. Executable cells contain no imaging download or transfer call. These results validate the local source candidate and a small live metadata sample; they do not establish package publication, full server compatibility, or clinical completeness.
