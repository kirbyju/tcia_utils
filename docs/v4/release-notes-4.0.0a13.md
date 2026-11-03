# 4.0.0a13 preview

The downloads/examples extras now require the published tcia-data-retriever
0.1.0rc2 wheel. RC1 inherited child process file descriptors, so Colab/Jupyter
could show only SDK preparation messages. RC2 relays both child streams through
Python stdout/stderr while the transfer is running.

The SDK requests TDR's plain progress snapshots, normally paced about every five
seconds, plus phase/final messages. It does not implement another renderer or
subprocess launcher. Failure and cancellation remain errors. The existing
progress=False option controls SDK messages only; TDR progress remains enabled.

Tests exercise the real released Python wrapper with a synthetic child CLI and
prove output reaches notebook-style capture before completion, including failure.
CI explicitly runs these tests with the downloads extra installed.

RC2 also uses supplied CLI/Python controlled keys for that invocation only, with
no saved credential copy or GUI-key fallback. Current guide/notebook wording is
updated accordingly; no live controlled payload transfer is claimed.

Restart a Colab/Jupyter runtime that already imported RC1 after upgrading, then
run from the top. No server, metadata-bundle or output-layout update is required.
