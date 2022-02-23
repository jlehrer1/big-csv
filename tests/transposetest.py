import chunk
import numpy as np 
import pandas as pd 
import pathlib 
import os 

from transposecsv import Transpose 

here = pathlib.Path(__file__).parent.resolve()
print(f'Here in transposetest.py file is {here =}')
print('Generating test DataFrame')
rand = np.ones([500, 1000])
rand = pd.DataFrame(rand)

rand.to_csv(os.path.join(here, 'test.csv'), index=False)

rand = rand.T.reset_index(drop=True)

print('Running transpose')
trans = Transpose(
    file=os.path.join(here, 'test.csv'),
    outfile=os.path.join(here, 'test_T.csv'),
    chunksize=10,
    save_chunks=True,
    quiet=False,
)

trans.compute()
print('Reading in transpose')
transpose = pd.read_csv(os.path.join(here, 'test_T.csv'))

transpose.columns = pd.RangeIndex(0, len(transpose.columns))
print(f'Shapes are {transpose.shape = }, {rand.shape = }')

print(f'Transposed == direct transpose: {transpose.equals(rand)}')

with open(os.path.join(here, '..', 'credentials')) as f:
    secret, access = f.readlines()

secret, access = secret.rstrip(), access.rstrip()

trans.upload(
    bucket='braingeneersdev',
    endpoint_url='https://s3.nautilus.optiputer.net',
    aws_secret_key_id=secret,
    aws_secret_access_key=access,
    remote_file_key='jlehrer/TEST_TRANSPOSE.csv',
    remote_chunk_path='jlehrer/TEST_TRANSPOSE_CHUNKS'
)