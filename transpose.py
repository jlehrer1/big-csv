from asyncore import write
import pandas as pd 
import numpy as np 
import os 
import argparse
import pathlib
import subprocess
from upload_helper import upload

here = pathlib.Path(__file__).parent.resolve()
def generate_parser(): 
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

    return parser

if __name__ == "__main__":
    parser = generate_parser()
    args = parser.parse_args()

    file = args.file
    chunksize = args.chunksize 
    sep = args.sep
    to_upload = args.upload 

    process = subprocess.Popen('wc -l {}'.format(file).split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    output = output.strip().decode('UTF-8')

    lines = int(output.split(' ')[0])
    print(f'File has {lines} lines and chunksize is {chunksize}')

    if not os.path.isdir(os.path.join(here, 'chunks')):
        print('Making chunk folder')
        os.mkdir(os.path.join(here, 'chunks'))

    chunksizes = []

    for df, l in zip(pd.read_csv(file, sep=sep, chunksize=chunksize), range(0, lines // chunksize + int(lines % chunksize == 0))):  # if we have one last small chunk or not 
        print(f'Working on chunk {l}')
        print(f'Chunk {l} before transpose has shape {df.shape}')
        df = df.T
        print(f'Chunk {l} after transpose has shape {df.shape}')

        print(f'Writing chunk {l} to csv')
        write_path = file.split('/')[-1] # if not in local path, just get the file name 
        df.to_csv(os.path.join(here, 'chunks', f'{write_path[:-4]}_{l}.csv'), sep=',', index=False)

        print(f'Uploading chunk {l} to S3')

        if to_upload:
            upload(
                os.path.join(here, 'chunks', f'{write_path[:-4]}_{l}.csv'),  #file name
                os.path.join('chunks', f'{write_path[:-4]}_{l}.csv') #remote name
            )

    print('Combining chunks')
    os.system(
        f"paste -d ',' {here}/chunks/* > {write_path}.csv && \
        tail -n +2 {write_path}.csv > {write_path}_clipped.csv && \
        mv {write_path}_clipped.csv {write_path}.csv"
    )
