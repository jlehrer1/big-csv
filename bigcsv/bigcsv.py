import os 
import boto3
import pandas as pd 
import anndata as an
import pathlib 
import warnings 

from scipy.sparse import csr_matrix 
from typing import Any 

def transpose_csv(
    file: str, 
    outfile: str, 
    insep: str, 
    outsep: str,
    chunksize: int, 
    chunkfolder: str,
    save_chunks: bool,
    quiet=bool, 
) -> None:
    """
    Calculates the transpose of a .csv too large to fit in memory 

    Parameters:
    file: Path to input file 
    outfile: Path to output file (transposed input file)
    insep: Separator for input file,  default is ','
    outsep: Separator for output file, default is ','
    chunksize: Number of lines per iteration
    chunkfolder: Path to chunkfolder
    quiet: Boolean indicating whether to print progress or not 

    Returns:
    None
    """

    # First, get the number of lines in the file (total number we have to process)
    with open(file) as f:
        lines = len(f.readlines())
    
    if not quiet: print(f'Number of lines to process is {lines - 1}')

    # Get just the outfile name for writing chunks
    outfile_split = outfile.split('/')
    outfile_name = outfile_split[-1][:-4] # takes /path/to/file.csv --> file 

    if not os.path.isdir(chunkfolder):
        if not quiet: print(f'Making chunk folder {chunkfolder = }')
        os.mkdir(chunkfolder)

    num_chunks = lines // chunksize + int(lines % chunksize == 0) # if we have one last small chunk or not 
    if not quiet: print(f'Total number of chunks is {num_chunks}')

    for df, l in zip(pd.read_csv(file, sep=insep, chunksize=chunksize), range(0, num_chunks + 1)):  
        if not quiet: print(f'Working on chunk {l} out of {num_chunks}')
        df = df.T

        if not quiet: print(f'Writing chunk {l} to csv')
        df.to_csv(os.path.join(chunkfolder, f'chunk_{outfile_name}_{l}.csv'), sep=outsep, index=False)

    if not quiet: print(f'Combining chunks from {chunkfolder} into {outfile}')

    os.system(
        f"paste -d ',' {chunkfolder}/* > {outfile}"
    )

    if not save_chunks:
        if not quiet: print('Finished combining chunks, deleting chunks.')
        os.system(
            f"rm -rf {chunkfolder}/*"
        )

    if not quiet: print('Done.')

def to_h5ad(
    file: str,
    outfile: str,
    sep: str,
    sparsify: bool,
    chunksize: int,
    chunkfolder: str,
    save_chunks: bool,
    quiet: bool,
    compression: str,
    index_col: str,
    lines: int,
    dtype: Any,
    index: bool,
) -> None:

    if lines is None:
        with open(file) as f:
            lines = len(f.readlines())
        
    num_chunks = lines // chunksize + int(lines % chunksize == 0)
    if not quiet: print(f'Chunkifying .csv file with {num_chunks = }')
    for df, l in zip(pd.read_csv(file, chunksize=chunksize, compression=compression, dtype=dtype, index_col=index_col, sep=sep), range(0, num_chunks + 1)):  
        if not quiet: print(f'Writing {l = }/{num_chunks}')
        df.to_csv(os.path.join(chunkfolder, f'chunk_{l}.csv'), index=index, header=None) # anndata doesn't need a header like Pandas does
            
    if not quiet: print('Reading in as h5ad')
    adata = an.read_csv(os.path.join(chunkfolder, f'chunk_0.csv'))
    
    for l in range(1, num_chunks + 1):
        if not quiet: print(f'Converting {l = }/{num_chunks}')
        andf = an.read_csv(os.path.join(chunkfolder, f'chunk_{l}.csv'))
        adata = an.concat([adata, andf])
        del andf # Remove from memory?
    
    # If we want to convert raw data to csr format 
    if sparsify:
        adata.X = csr_matrix(adata.X)
        
    if not quiet: print('Writing h5ad')
    adata.write(outfile)
    
    if not save_chunks:
        if not quiet: print('Deleting chunks')
        for l in range(0, num_chunks + 1):
            if not quiet: print(f'Deleting chunk {l = }')
            os.remove(
                os.path.join(chunkfolder, f'chunk_{l}.csv')
            )

def experimental_to_h5ad(
    file: str,
    outfile: str,
    sep: str,
    sparsify: bool,
    chunksize: int,
    quiet: bool=False,
    compression: str='infer',
    index_col: str=None,
    dtype: Any=None,
):
    chunkified = pd.read_csv(
        file, 
        chunksize=chunksize, 
        index_col=index_col, 
        compression=compression, 
        dtype=dtype,
        sep=sep,
    )

    with open(file) as f:
        lines = len(f.readlines()) - 1

    anndatas = []
    num_chunks = lines // chunksize + int(lines % chunksize == 0)

    for chunk, data in zip(range(0, num_chunks + 1), chunkified):
        if not quiet: print(f'Working on chunk {chunk}/{num_chunks}')

        if sparsify:
            df = an.AnnData(
                X=csr_matrix(data.values),
            )

            df.var.index = data.columns.values 
            df.obs.index = data.index.values
        else:
            df = an.AnnData(data)

        anndatas.append(df)

    if not quiet: print('Concatenating h5ad\'s')
    df = an.concat(anndatas)
    df.var = data.var.reset_index(drop=True)
    
    if not quiet: print('Writing h5ad to file')
    df.write_h5ad(outfile)

class BigCSV:
    def __init__(
        self,
        file: str, 
        outfile: str=None, 
        insep: str=',', 
        outsep: str=',',
        chunksize: str=400, 
        save_chunks: bool=False,
        quiet: bool=False,
        chunkfolder: str=None,
    ):
        self.file = file 
        self.outfile = outfile
        self.insep = insep 
        self.outsep = outsep
        self.chunksize = chunksize
        self.save_chunks = save_chunks
        self.quiet = quiet
        self.chunkfolder = chunkfolder

        outfile_split = file.split('/')
        self.outfile_name = outfile_split[-1][:-4] #takes /path/to/file.csv --> file 

        if chunkfolder is None:
            if not quiet: print('Chunkfolder not passed, generating...')
            self.chunkfolder = pathlib.Path(file).stem 

        if not os.path.isdir(self.chunkfolder):
            if not self.quiet: print(f"Creating chunkfolder {self.chunkfolder}")
            os.makedirs(self.chunkfolder, exist_ok=True)

    def transpose_csv(
        self,
        outfile: str=None,
    ):
        if outfile is None and self.outfile is None:
            raise ValueError("Error, either self.outfile must not be None or outfile must not be None.")

        transpose_csv(
            file=self.file, 
            outfile=(outfile if outfile is not None else self.outfile), 
            insep=self.insep, 
            outsep=self.outsep,
            chunksize=self.chunksize, 
            chunkfolder=self.chunkfolder,
            save_chunks=self.save_chunks,
            quiet=self.quiet, 
        )

    def to_h5ad(
        self,
        outfile: str=None,
        sparsify: bool=False,
        compression: str='infer',
        lines: int=None,
        dtype: Any=None,
        index_col: str=None,
        index: bool=True,
    ):  
        if pathlib.Path(outfile).suffix != 'h5ad':
            warnings.warn('Suffix of outfile is not .h5ad, although it is being converted to an h5ad.')

        if outfile is None and self.outfile is None:
            raise ValueError("Error, either self.outfile must not be None or outfile must not be None.")

        # to_h5ad(
        #     file=self.file,
        #     outfile=(outfile if outfile is not None else self.outfile),
        #     sep=self.insep,
        #     chunksize=self.chunksize,
        #     chunkfolder=self.chunkfolder,
        #     save_chunks=self.save_chunks,
        #     quiet=self.quiet,
        #     sparsify=sparsify,
        #     compression=compression,
        #     lines=lines,
        #     dtype=dtype,
        #     index_col=index_col,
        #     index=index,
        # ) 

        experimental_to_h5ad(
            file=self.file,
            outfile=(outfile if outfile is not None else self.outfile),
            sep=self.insep,
            chunksize=self.chunksize,
            chunkfolder=self.chunkfolder,
            save_chunks=self.save_chunks,
            quiet=self.quiet,
            sparsify=sparsify,
            compression=compression,
            lines=lines,
            dtype=dtype,
            index_col=index_col,
            index=index,
        )

    
    def upload(
        self, 
        bucket: str,
        endpoint_url: str,
        aws_secret_key_id: str,
        aws_secret_access_key: str,
        remote_file_key: str=None,
        remote_chunk_path: str=None, 
    ) -> None:
        """Uploads the chunks and/or transposed file to the given S3 bucket.

        :param bucket: Bucket name
        :type bucket: str
        :param endpoint_url: S3 endpoint
        :type endpoint_url: str
        :param aws_secret_key_id: AWS secret key for your account
        :type aws_secret_key_id: str
        :param aws_secret_access_key: Specifies the secret key associated with the access key
        :type aws_secret_access_key: str
        :param remote_file_key: key to upload file to in S3. Must be complete path, including file name , defaults to None
        :type remote_file_key: str, optional
        :param remote_chunk_path: Optional, key to upload chunks to in S3. Must be a folder-like path, where the chunks will be labeled as chunk_{outfile_name}_{l}.csv, defaults to None
        :type remote_chunk_path: str, optional
        :raises ValueError: If chunks have been deleted but requested to be uploaded, then we have to error since there is nothing to upload.
        """

        if remote_chunk_path and not self.save_chunks: # if remote_chunk_path is not None and self.save_chunks=False, then we cannot upload!
            raise ValueError('Error, Transpose class not initialized with save_chunks=True, so chunks have been deleted. Rerun with save_chunks=True, or call upload method with remote_chunk_path=None.')
        
        # Defines upload function and uploades combined data after all chunks are generated
        s3 = boto3.resource(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_secret_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        remote_file_key = (self.file if not remote_file_key else remote_file_key)

        if remote_file_key:
            if not self.quiet: print(f'Uploading {self.outfile} transposed to {remote_file_key}')
            s3.Bucket(bucket).upload_file(
                file=self.outfile,
                Key=remote_file_key,
            )

        if remote_chunk_path:
            if not self.quiet and remote_chunk_path: print(f'Uploading chunks to {remote_chunk_path}')
            for file in os.listdir(self.chunkfolder):
                if not self.quiet: print(f'Uploading {file}')
                
                s3.Bucket(bucket).upload_file(
                    file=os.path.join(self.chunkfolder, file),
                    Key=os.path.join(remote_chunk_path, file)
                )

    def __repr__(self) -> str:
        return f"file={self.file}, outfile={self.outfile}, chunksize={self.chunksize}"

    def __str__(self) -> str:
        return self.__repr__()