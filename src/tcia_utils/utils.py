import pandas as pd

def searchDf(search_term, column_name=None, dataframe_var='df'):
    # If the custom dataframe variable name is provided, use that to get the dataframe
    if dataframe_var in globals():
        df = globals()[dataframe_var]
    else:
        raise ValueError(f"Dataframe variable '{dataframe_var}' not found in the global namespace.")

    if column_name:
        result = df[df[column_name].astype(str).str.contains(search_term, case=False)]
    else:
        result = df[df.apply(lambda row: any(row.astype(str).str.contains(search_term, case=False)), axis=1)]
    return result