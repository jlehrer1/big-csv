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

for df, l in zip(pd.read_csv(file, sep='\t', chunksize=chunksize), range(0, lines // chunksize)): 
    print(f'Working on chunk {l}')
    print(f'Chunk {l} has shape {df.shape}')
    df2 = df.T
    df2.to_csv(os.path.join('chunks', f'{l}.csv'), sep=',', index=False)