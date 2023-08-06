import pandas as pd
import inspect


def searchDf(search_term, column_name=None, dataframe=None):
    if dataframe is not None:
        df = dataframe
    else:
        caller_frame = inspect.currentframe().f_back
        caller_globals = caller_frame.f_globals

        if 'df' in caller_globals:
            df = caller_globals['df']
        else:
            raise ValueError("Dataframe variable 'df' not found in the global namespace.")

    if column_name:
        result = df[df[column_name].astype(str).str.contains(search_term, case=False)]
    else:
        result = df[df.apply(lambda row: any(row.astype(str).str.contains(search_term, case=False)), axis=1)]
    return result