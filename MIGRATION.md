# Migration Guide: Moving to the new `nbia` module

This guide outlines the key changes introduced in the latest `tcia_utils` update, where the `nbia.py` module has been rewritten to use the new NBIA V4 APIs. These changes simplify the library and align it with modern data access practices.

## Key Changes

### 1. Removal of Authentication and Token Management

The most significant change is the removal of all authentication-related functions. Due to recent changes in NIH policies, TCIA no longer hosts controlled-access data in NBIA. As a result, the new NBIA V4 API, which this library now uses, does not require user authentication. More information about this policy change can be found [here](https://www.cancerimagingarchive.net/nih-controlled-data-access-policy/).

The following functions have been **removed**:

-   `getToken()`
-   `refreshToken()`
-   `makeCredentialFile()`

You no longer need to manage API tokens or credentials within your code. Simply remove any calls to these functions.

### 2. Renamed and Simplified Functions

-   `getSimpleSearchWithModalityAndBodyPartPaged()` has been renamed to **`getSimpleSearch()`**. The functionality remains the same, but the name is now more concise.

### 3. Deprecated and Moved Visualization Functions

The following visualization functions have been moved to a separate package, `simpleDicomViewer`, to better separate concerns:

-   `viewSeries()`
-   `viewSeriesAnnotation()`

You can install it via pip:
`pip install simpleDicomViewer`

The `makeVizLinks()` function has been deprecated in favor of the new `idcOhifViewer()` function, which provides a more robust and feature-rich way to visualize studies and series using the IDC OHIF viewer. It can take either a DataFrame or a list of series and generates clickable links for visualization.

## How to Update Your Code

1.  **Remove Authentication Calls**: Delete any lines of code that call `getToken()`, `refreshToken()`, or `makeCredentialFile()`.

2.  **Update Function Calls**:
    -   Replace `nbia.getSimpleSearchWithModalityAndBodyPartPaged(...)` with `nbia.getSimpleSearch(...)`.
    -   For `viewSeries()` and `viewSeriesAnnotation()`, install the `simpleDicomViewer` package and update your imports.
    -   Replace any usage of `makeVizLinks()` with `idcOhifViewer()`.

## Backwards Compatibility

To ensure a smoother transition, we have implemented a compatibility layer. If you call a removed or renamed function, a `DeprecationWarning` will be raised, guiding you on how to update your code. For renamed functions, the call will be automatically forwarded to the new function, so your code won't break immediately. However, we strongly recommend updating your code to use the new function names to avoid issues in the future.