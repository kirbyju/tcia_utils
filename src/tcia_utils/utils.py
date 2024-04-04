import pandas as pd
import inspect
from bs4 import BeautifulSoup
from unidecode import unidecode

def searchDf(search_term, column_name=None, dataframe=None):
    """
    Helper function to filter dataframes.
    It allows you to specify a search_term, the column_name 
    (optional: if you'd like to narrow the search to a single column),
    and the variable name of the dataframe object to filter.  
    It assumes the dataframe variable is df if no dataframe name
    is specified.
    """
    
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
    # reset the index
    result = result.reset_index(drop=True)
    return result


def format_disk_space_binary(size_in_bytes):
    """
    Helper function for reportCollections() to format bytes to other binary units.
    I.e. Mebibytes (MiB) reported in Windows.
    """
    if size_in_bytes < 1024 ** 2:
        return f'{size_in_bytes / 1024:.2f} KB'
    elif size_in_bytes < 1024 ** 3:
        return f'{size_in_bytes / (1024 ** 2):.2f} MB'
    elif size_in_bytes < 1024 ** 4:
        return f'{size_in_bytes / (1024 ** 3):.2f} GB'
    elif size_in_bytes < 1024 ** 5:
        return f'{size_in_bytes / (1024 ** 4):.2f} TB'
    else:
        return f'{size_in_bytes / (1024 ** 5):.2f} PB'
        

def format_disk_space(size_in_bytes):
    """
    Helper function for reportCollections() to format bytes to other units.
    I.e. Megabytes (MB) reported in Mac/Linux.
    """
    if size_in_bytes < 1000:
        return f'{size_in_bytes:.2f} B'
    elif size_in_bytes < 1000 ** 2:
        return f'{size_in_bytes / 1000:.2f} kB'
    elif size_in_bytes < 1000 ** 3:
        return f'{size_in_bytes / (1000 ** 2):.2f} MB'
    elif size_in_bytes < 1000 ** 4:
        return f'{size_in_bytes / (1000 ** 3):.2f} GB'
    elif size_in_bytes < 1000 ** 5:
        return f'{size_in_bytes / (1000 ** 4):.2f} TB'
    else:
        return f'{size_in_bytes / (1000 ** 5):.2f} PB'
    

def remove_html_tags(text):
    """
    Helper function to convert HTML to plain text.
    """
    soup = BeautifulSoup(text, 'html.parser')
    plain_text = soup.get_text().strip()
    clean_text = unidecode(plain_text)  # Apply unidecode to remove or replace non-ASCII characters
    return clean_text