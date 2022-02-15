import chunk
import numpy as np 
import pandas as pd 
import pathlib 
import os 

from transposecsv import Transpose 

here = pathlib.Path(__file__).parent.resolve()

print('Generating test DataFrame')
rand = np.ones([500, 1000])
rand = pd.DataFrame(rand)
rand.to_csv(os.path.join(here, 'test.csv'), index=False)

print('Running transpose')
trans = Transpose(
    file=os.path.join(here, 'test.csv'),
    outfile=os.path.join(here, 'test_T.csv'),
    chunksize=100,
)

trans.compute()
print('Reading in transpose')

transpose = pd.read_csv(os.path.join(here, 'test_T.csv'))
print(f'Transposed == direct transpose: {transpose.equals(rand.T)}')