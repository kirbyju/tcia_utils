import pandas as pd

def searchDf(search_term, column_name=None, dataframe=df,):
    if column_name:
        result = df[df[column_name].astype(str).str.contains(search_term, case=False)]
    else:
        result = df[df.apply(lambda row: any(row.astype(str).str.contains(search_term, case=False)), axis=1)]
    return result