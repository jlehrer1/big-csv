from genericpath import exists
import pandas as pd 
import numpy as np 
import os 
import argparse
import pathlib 
import subprocess

here = pathlib.Path(__file__).parent.resolve()

parser = argparse.ArgumentParser(description='Calculate the transpose of a large file')
parser.add_argument('-file', metavar='F', type=str, help='Path to the input file')
parser.add_argument('-chunksize', type=int, help='Chunksize to use, equivalently, number of rows to read into memory at once.')
parser.add_argument('-sep', type=str, help='File seperator. Should be either \',\' for csv or \'\\t\' for tsv')

args = parser.parse_args()
file = args.file
chunksize = args.chunksize 
sep = args.sep

process = subprocess.Popen('wc -l {}'.format(file).split(), stdout=subprocess.PIPE)
output, error = process.communicate()
output = output.strip().decode('UTF-8')

lines = int(output.split(' ')[0])
print(f'File has {lines} lines and chunksize is {chunksize}')

if not os.path.isdir(os.path.join(here, 'chunks')):
    print('Making chunk folder')
    os.mkdir(os.path.join(here, 'chunks'))

chunksizes = []
for df in pd.read_csv(file, chunksize=chunksize):
    print(f'Working on chunk {df}')
    df = df.T
    df.to_csv(os.path.join('chunks', f'{df}.tsv'), sep='\t', index=False)


# for c in range(chunksize, lines, lines // chunksize):
#     chunksizes.append(c)
#     print(f'Chunk {c}')
#     df = pd.read_csv(file, nrows=c, sep=sep)
#     df = df.T
#     df.to_csv(os.path.join('chunks', f'chunk_{c}.tsv'), sep='\t')

