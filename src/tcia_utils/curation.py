import pandas as pd
from datetime import datetime
import logging
from tcia_utils.utils import searchDf
import os
import numpy as np
import nibabel as nib
import nilearn.plotting as nlp
import matplotlib.pyplot as plt
from nilearn.image import resample_img
import hashlib
from collections import defaultdict

_log = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s'
    , level=logging.INFO
)


def niftiDups(data_dir, format=None):
    """
    Allows you to specify a folder and it will compare the hashes of
    *.nii or *.nii.gz files to see if any contain the same image pixel
    content.  The first column of the output will contain the hash values
    and the second column contains the file paths.  Files with duplicate
    content will have the same hash value in the first column.

    Results will be returned as a dataframe or you can optionally specify 
    format = "csv" as a parameter to save a CSV file.  

    Windows users should specify paths with forward slashes, e.g. "C:/data/".
    """
    def calculate_image_hash(file_path):
        try:
            nifti_img = nib.load(file_path)
            image_data = nifti_img.get_fdata()
            return hashlib.sha256(image_data.tobytes()).hexdigest()
        except Exception as e:
            _log.error(f"Error processing {file_path}: {str(e)}")
            return None

    # Create a dictionary to store hashes and corresponding file paths
    hashes = defaultdict(list)

    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.nii') or file.endswith('.nii.gz'):
                file_path = os.path.join(root, file)
                image_hash = calculate_image_hash(file_path)
                if image_hash:
                    hashes[image_hash].append(file_path)

    # Create a list of duplicate files
    duplicate_files = []

    # Create a list of DataFrames to concatenate
    df_list = []

    # Display a summary of duplicates and populate the DataFrame
    for image_hash, file_paths in hashes.items():
        if len(file_paths) > 1:
            _log.warning(f"Duplicate content found in these files:")
            for file_path in file_paths:
                _log.warning(f"{file_path}")
                df_list.append(pd.DataFrame({'Hash': [image_hash], 'File Path': [file_path]}))
            duplicate_files.extend(file_paths)

    # Concatenate the DataFrames
    df = pd.concat(df_list, ignore_index=True)

    # Create a CSV file if format is specified as "csv"
    if format == "csv":
        # Get the current date and time
        current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M')

        # Generate the CSV file name with the date and time
        csv_file_name = f'nifti_duplicates_{current_datetime}.csv'

        # Save the dataframe to a CSV file with the generated name
        df.to_csv(csv_file_name, index=False)
        _log.info(f"CSV file created: {csv_file_name}")

    return df
    

def niftiHeaderAnalysis(path, unique=None, format=None):
    """
    Specify the path of a directory to generate a dataframe or spreadsheet 
    of header data from all .nii or *.nii.gz files in that directory. 
    Specify format = "csv" to create the spreadsheet.
    """
    def extract_all_nifti_metadata(filepath):
        try:
            nifti_img = nib.load(filepath)
            header = nifti_img.header
            metadata = {
                'Filename': os.path.basename(filepath),
                'Path': filepath
            }

            # Iterate through all available header fields
            for field in header.keys():
                metadata[field] = header[field]

            return metadata
        except Exception as e:
            _log.error(f"Error processing {filepath}: {str(e)}")
            return None

    # Main script
    output_dataframes = []

    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.nii') or file.endswith('.nii.gz'):
                file_path = os.path.join(root, file)
                metadata = extract_all_nifti_metadata(file_path)
                if metadata:
                    output_dataframes.append(pd.DataFrame([metadata]))

    # Concatenate all dataframes into one
    output_dataframe = pd.concat(output_dataframes, ignore_index=True)

    if unique == 'yes':
        # Create a new dataframe to store unique values
        unique_dataframe = pd.DataFrame()

        # Iterate through each column
        for column in output_dataframe.columns:
            unique_values = output_dataframe[column].astype(str).unique()

            # Create a dataframe with unique values for the current column
            unique_column_df = pd.DataFrame({column: unique_values})

            # Concatenate the unique values dataframe with the unique dataframe
            unique_dataframe = pd.concat([unique_dataframe, unique_column_df], axis=1)
        
        # rename before returning df or creating csv
        output_dataframe = unique_dataframe

    if format == 'csv':
        # Get the current date and time
        current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M')

        # Generate the CSV file name with the date and time
        csv_file_name = f'nifti_metadata_{current_datetime}.csv'

        # Save the dataframe to a CSV file with the generated name
        output_dataframe.to_csv(csv_file_name, index=False)
        _log.info(f"CSV file created: {csv_file_name}")

    return output_dataframe
        

def niftiHeaderEdit(file_path, tag, new_value, input_type=None):
    """
    Specify the path of a .nii or *.nii.gz file along with the name
    of the tag and value you'd like to change it to.
    
    Bulk edits can be performed by specifying input_type as
    "df" for dataframe or "csv" for a CSV file. In this case the file_path
    should be the variable name of the dataframe, or the path to the
    CSV file.  The input data for bulk edits should contain a "Path"
    column to specify the file locations for each file you want to edit.
    """
    try:
        if input_type is None:
            # Process a single file
            file_paths = [file_path]
        elif input_type == "df":
            # Load DataFrame
            df = file_path
            file_paths = df['Path'].tolist()
        elif input_type == "csv":
            # Load CSV file
            df = pd.read_csv(file_path)
            file_paths = df['Path'].tolist()
        else:
            raise ValueError("Invalid input_type. Must be 'df' or 'csv' to process multiple files.")
        
        for file_path in file_paths:
            # Load the NIfTI image
            img = nib.load(file_path)
            
            # Access the header
            header = img.header
            
            # Set the new value for the specified tag
            header[tag] = new_value
            
            # Save the modified NIfTI image
            nib.save(img, file_path)
            
            _log.info(f"Header tag '{tag}' in file '{file_path}' has been updated to '{new_value}'.")
        
    except Exception as e:
        _log.error(f"Error editing NIfTI header: {e} for '{file_path}'.")
        

def nifti2png(inputDir, outputDir=None):
    """
    Specify an input directory containing .nii and .nii.gz files to be
    converted into a 3x3 grid of PNG files.  Resulting files are saved
    in a new folder, "pngOutput", unless outputDir is specified.
    """
    try:
        # List of NIfTI files that you want to process
        nifti_files = [os.path.join(root, file) for root, dirs, files in os.walk(inputDir) for file in files if file.endswith('.nii') or file.endswith('.nii.gz')]

        # Create a directory to store the PNG images if outputDir is not specified
        if outputDir is None:
            outputDir = os.path.join(os.getcwd(), "pngOutput")

        # Create the output directory if it doesn't exist
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)

        # Set the opacity (alpha) for the mask overlay
        opacity = 1

        # Iterate through rows in the CSV file
        for file in nifti_files:
            image_path = file

            # Load the NIfTI image
            image = nib.load(image_path)

            # Get the file name without the extension for the title and output file
            image_file_name = os.path.splitext(os.path.basename(image_path))[0]

            # Create a figure for the image
            fig, axes = plt.subplots(3, 3, figsize=(9, 9))
            fig.suptitle(f"{file}", color='white')

            for i in range(9):
                row_index, col_index = divmod(i, 3)
                slice_index = int(i * image.shape[-1] / 9)

                # Get the slice from the image and mask using "..."
                image_slice = image.dataobj[..., slice_index]

                # Display the slice
                axes[row_index, col_index].imshow(image_slice, cmap='gray')
                axes[row_index, col_index].axis('off')
                axes[row_index, col_index].set_title(f"Slice {slice_index}", color='white')

            # Save the plot as a PNG file
            output_file = os.path.join(outputDir, f"{os.path.basename(os.path.dirname(image_path))}_{image_file_name}.png")
            plt.savefig(output_file, bbox_inches='tight', pad_inches=0, format='png', dpi=300, facecolor='black')
            
            # Close the figure
            plt.close()
    except KeyError:
        _log.error(f"KeyError occurred while processing {file}.")
    except Exception as e:
        _log.error(f"Error processing {file}: {str(e)}")
