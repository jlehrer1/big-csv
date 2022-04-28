# bigcsv: A small Python library to manipulate large csv files that can't fit in memory. 

## Transposition
`bigcsv` allows for easy calculation of csv transposes, even when the csv is much too large to fit in memory. 

## Converting to h5ad 
If data is purely numeric, it is much more efficient to store in in `h5ad` (readable by `AnnData`), which uses the amazing HDF5 format under-the-hood.

## Installation

To install, run `pip install bigcsv`

## How to use  
All operations are method of the `BigCSV` class, which contains metadata information used to do all calculations.

```python
from bigcsv import BigCSV

obj = BigCSV(
    file='massive_dataset.csv',
    chunksize=400, # Number of rows to read in at each iteration
    # leave as default
    # insep=',', 
    # outsep=',',
    # chunksize=400, 
    # save_chunks=False,
    # quiet=False,
)

obj.to_h5ad(outfile='converted.h5ad')

# Or maybe we want to keep as csv, but transpose it (in the case of non-numerical data)
obj.transpose(outfile='dataset_T.csv')
```

## Documentation
1. `bigcsv.BigCSV`
Class containing methods for manipulating csvs.     
### Parameters:  

`file`: Path to input file   
`outfile`: Path to output file (transposed input file)  
`insep=','`: Input separator for delimited file, by default is `,`
`outsep=','`: Output separator for delimited file (in the case of csv --> csv operations)
`chunksize=400`: Number of lines per iteration  
`save_chunks`: To save intermediate chunks or not 
`chunkfolder=None`: Optional, Path to chunkfolder  
`quiet=False`: Boolean indicating whether to print progress or not

2. `bigcsv.BigCSV.transpose_csv`  
### Parameters:  
`outfile=None`: Ouput file to write to, or if specified in initialization, writes to that file name

3. `bigcsv.BigCSV.to_h5ad`
### Parameters
`outfile=None`: Ouput file to write to, or if specified in initialization, writes to that file name
`sparsify: bool=False`: Sparsify rows in h5 matrix 
`compression: str='infer'`: Compression format of input csv, if compressed. Probably just leave to infer unless the filename is weird. 
`lines: int=None`: Number of lines in the file. If you know a priori, this saves some time. Also cannot be calculated for compressed files. 
`dtype: Any=None`: dtype of entries of input matrix 
`index_col: str=None`: Column of input csv to use as index, if any. 
`index: bool=True`: Save index when converting to h5ad. 