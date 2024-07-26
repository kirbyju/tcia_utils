import pandas as pd
import logging
from bs4 import BeautifulSoup
from unidecode import unidecode

_log = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s',
    level=logging.INFO
)

def searchDf(search_term, dataframe=None, column_name=None):
    """
    This function searches for a term or a list of terms in a specified dataframe and column.
    
    Parameters:
    search_term (str or list): The term or list of terms to search for.
    dataframe (pd.DataFrame, optional): The dataframe to search in. Defaults to None.
    column_name (str, optional): The name of the column to restrict the search to. If not provided, the search is performed across all columns.

    Returns:
    pd.DataFrame: A dataframe containing the rows where the search term was found. Returns None if an error occurs.
    """

    # If search_term is a string, convert it to a list
    if isinstance(search_term, str):
        search_term = [search_term]

    # If dataframe is None, log an error and return None
    if dataframe is None:
        _log.error("No dataframe provided.")
        return None

    # If column_name is provided, restrict the search to that column
    if column_name:
        if column_name not in dataframe.columns:
            _log.error(f"No column named '{column_name}' found in the dataframe.")
            return None
        contains_values = dataframe[column_name].apply(
            lambda x: any(str(value).lower() in str(x).lower() for value in search_term)
        )
    else:
        contains_values = dataframe.apply(
            lambda row: any(any(str(value).lower() in str(cell).lower() for value in search_term) for cell in row),
            axis=1
        )

    df_with_values = dataframe[contains_values]

    return df_with_values


def copy_df_cols(df_to_update, columns_to_copy, source_df, key_column):
    """
    Create a new dataframe which includes specified columns copied
    from source_df to df_to_update based on a key_column used as the lookup.
    
    Parameters:
    df_to_update (pd.DataFrame): The dataframe that needs to be updated.
    columns_to_copy (str or list): The column(s) to copy from source_df to df_to_update. Can be a single column name (str) or a list of column names.
    source_df (pd.DataFrame): The dataframe with the values you want to copy.
    key_column (str): The column name to use for matching the rows between the two dataframes.
    
    Returns:
    pd.DataFrame: The updated dataframe.
    """
    # Create a copy of df_to_update to avoid modifying the original DataFrame
    df_to_update_copy = df_to_update.copy()
    
    # If columns_to_copy is a string, convert it to a list
    if isinstance(columns_to_copy, str):
        columns_to_copy = [columns_to_copy]

    for column_to_copy in columns_to_copy:
        # Ensure the column_to_copy exists in the source_df
        if column_to_copy not in source_df.columns:
            _log.error(f"Column '{column_to_copy}' does not exist in the source DataFrame.")
            # If the column doesn't exist in source_df, create it in df_to_update with NaN values
            df_to_update[column_to_copy] = pd.NA
        else:
            # Merging df_to_update with the specified column from source_df using the key_column as the key
            updated_df = df_to_update.merge(source_df[[key_column, column_to_copy]], 
                                            on=key_column, 
                                            how='left', 
                                            suffixes=('', '_lookup'))

            # Check if the merged column exists before combining
            lookup_col = f"{column_to_copy}_lookup"
            if lookup_col in updated_df.columns:
                # Updating the specified column in df_to_update with the values from source_df
                updated_df[column_to_copy] = updated_df[lookup_col].combine_first(updated_df[column_to_copy])

            df_to_update = updated_df  # Update the df_to_update with merged results

    return df_to_update


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