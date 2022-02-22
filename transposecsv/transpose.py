from asyncore import write
import pandas as pd 
import numpy as np 
import os 
import argparse
import pathlib
import subprocess

from .upload_helper import upload

here = pathlib.Path(__file__).parent.resolve()

def generate_parser(): 
    """
    Generates argparser, if this file is being run as a script. 

    Returns:
    argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(description='Calculate the transpose of a large file')
    parser.add_argument(
        '--file', 
        type=str, 
        help='Path to the input file', 
        required=True
    )

    parser.add_argument(
        '--chunksize',
        type=int,
        help='Chunksize to use, equivalently, number of rows to read into memory at once.', 
        required=False,
        default=400,
    )

    parser.add_argument(
        '--sep', 
        type=str,
        help='File seperator. Should be either \',\' for csv or \'\\t\' for tsv',
        required=True
    )

    parser.add_argument(
        '--upload',
        type=bool,
        help='To upload use --upload, otherwise use --no-upload',
        action=argparse.BooleanOptionalAction,
    )

    parser.add_argument(
        '--outfile',
        type=str,
        help='Path to outfile',
        required=False,
        default=None
    )

    return parser

def transpose_file(
    file: str, 
    outfile: str, 
    insep: str, 
    outsep: str,
    chunksize: int, 
    credentials_file: str, 
    to_upload: bool,
    save_chunks: bool,
    quiet=bool, 
) -> None:
    """
    Calculates the transpose of a .csv too large to fit in memory 

    Parameters:
    file: Path to input file 
    outfile: Path to output file (transposed input file)
    sep: Separator for .csv, by default is ,
    chunksize: Number of lines per iteration
    credentials_file: Path to S3 credentials file, in the case where we upload chunks and final tranposed file 
    to_upload: Boolean indicating whether or not to upload to S3
    quiet: Boolean indicating whether to print progress or not 

    Returns:
    None
    """

    # First, get the number of lines in the file (total number we have to process)
    with open(file) as f:
        lines = len(f.readlines())
    
    if not quiet: print(f'Number of lines to process is {lines}')

    # Get just the outfile name for writing chunks
    outfile_split = outfile.split('/')
    outfile_name = outfile_split[-1][:-4] # takes /path/to/file.csv --> file 

    if len(outfile_split) == 1: # as in there was no /path/to/file.csv, just file.csv
        chunkfolder = f'chunks_{outfile_name}'
    else:
        outfile_path = f"/{os.path.join(*outfile.split('/')[:-1])}"
        chunkfolder = os.path.join(outfile_path, f'chunks_{outfile_name}')

    if not os.path.isdir(chunkfolder):
        if not quiet: print(f'Making chunk folder {chunkfolder = }')
        os.mkdir(chunkfolder)

    num_chunks = lines // chunksize + int(lines % chunksize == 0) # if we have one last small chunk or not 
    if not quiet: print(f'Total number of chunks is {num_chunks}')

    for df, l in zip(pd.read_csv(file, sep=insep, chunksize=chunksize), range(0, num_chunks + 1)):  
        if not quiet: print(f'Working on chunk {l} out of {num_chunks}')
        df = df.T

        if not quiet: print(f'Writing chunk {l} to csv')
        df.to_csv(os.path.join(chunkfolder, f'{outfile_name}_{l}.csv'), sep=outsep, index=False)

        if to_upload:
            if not quiet: print(f'Uploading chunk {l} to S3')
            upload(
                file_name=os.path.join(chunkfolder, f'{outfile_name}_{l}.csv'),  #file name
                credential_file=credentials_file,
                remote_name=os.path.join(chunkfolder, f'{outfile_name}_{l}.csv') #remote name
            )

    if not quiet: print(f'Combining chunks from {chunkfolder} into {outfile}')
    os.system(
        f"paste -d ',' {chunkfolder}/* > {outfile}"
    )

    if not save_chunks:
        if not quiet: print('Finished combining chunks, deleting chunks.')
        os.system(
            f'rm -rf {chunkfolder}/*'
        )

    if not quiet: print('Done.')