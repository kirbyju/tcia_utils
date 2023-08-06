import pandas as pd

def searchDf(search_term, column_name=None, dataframe=None):
    if dataframe is None:
        # If dataframe is not provided, assume the dataframe is named 'df'
        if 'df' in globals():
            df = globals()['df']
        else:
            raise ValueError("Dataframe variable 'df' not found in the global namespace.")
    else:
        df = dataframe

    if column_name:
        result = df[df[column_name].astype(str).str.contains(search_term, case=False)]
    else:
        result = df[df.apply(lambda row: any(row.astype(str).str.contains(search_term, case=False)), axis=1)]
    return result




