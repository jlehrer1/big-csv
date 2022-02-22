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
    remote_name='jlehrer/massive_dataset_T.csv'
)
```
