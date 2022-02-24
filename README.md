# transposecsv: A small Python library to transpose large csv files that can't fit in memory. 

Suppose you have an `p x m` matrix where your original data is `m` points samples with `p` features, or in `m` points in `p` dimensional space. Then we want the column space to be the features, that is, we'd like to consider the `m x p` data matrix. This small library is for performing this calculation on arbitrarily large csv files.

It works in the following way:
1. Read in chunks that fit in memory
2. Transpose those in memory (which is fast)
3. Write each transposed chunk to a `.csv` file
4. Use `paste` to join the files horizontally (columnwise), this is why we don't need to save the index, since it will be the same as the columns of the original file. 

This process outputs the `m x p` matrix, as desired. This is particularly useful for single-cell data, where expression matrices are often uploaded genewise, but you may want to work with machine learning models that learn cellwise :). 

## Installation

To install, run `pip install transposecsv`

## How to use  
The transpose operation is contained in a lazily-loaded `Transpose` class, so the transpose operation isn't started on initialization. For example:

```python
from transposecsv import Transpose 

transpose = Transpose(
    file_name='massive_dataset.csv',
    write_path='massive_dataset_T.csv',
    chunksize=400, # Number of rows to read in at each iteration
    # leave as default
    # insep=',', 
    # outsep=',',
    # chunksize=400, 
    # save_chunks=False,
    # quiet=False,
)

transpose.compute()
```

Then to upload to S3, we would run 
```python
tranpose.upload(
    bucket='braingeneersdev',
    endpoint_url='https://s3.nautilus.optiputer.net',
    aws_secret_key_id=secret,
    aws_secret_access_key=access,
    remote_file_name='jlehrer/massive_dataset_T.csv'
)
```

## Documentation
1. `transposecsv.Transpose`  
### Parameters:  
`file`: Path to input file   
`outfile`: Path to output file (transposed input file)  
`sep=','`: Separator for .csv, by default is `,`
`chunksize=400`: Number of lines per iteration  
`chunkfolder=None`: Optional, Path to chunkfolder  
`quiet=False`: Boolean indicating whether to print progress or not

2. `transposecsv.Transpose.upload`  
### Parameters:  
`bucket`: Bucket name  
`endpoint_url`: S3 endpoint  
`aws_secret_key_id`: AWS secret key for your account   
`aws_secret_access_key`: Specifies the secret key associated with the access key  
`remote_file_key`: Optional, key to upload file to in S3. Must be complete path, including file name   
`remote_chunk_path`: Optional, key to upload chunks to in S3. Must be a folder-like path, where the chunks will be labeled as chunk_{outfile_name}_{l}.csv  
