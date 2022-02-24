from multiprocessing.sharedctypes import Value
import os 
import boto3
import pandas as pd 
from .transpose import transpose_file

def transpose_file(
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

class Transpose:
    def __init__(
        self,
        file: str, 
        outfile: str, 
        insep: str=',', 
        outsep: str=',',
        chunksize: str=400, 
        save_chunks: bool=False,
        quiet: bool=False,
    ):
        self.file = file 
        self.outfile = outfile
        self.insep = insep 
        self.outsep = outsep
        self.chunksize = chunksize
        self.save_chunks = save_chunks
        self.quiet = quiet

        outfile_split = outfile.split('/')
        outfile_name = outfile_split[-1][:-4] #takes /path/to/file.csv --> file 

        if len(outfile_split) == 1: #as in there was no /path/to/file.csv, just file.csv
            self.chunkfolder = f'chunks_{outfile_name}'
        else:
            outfile_path = f"/{os.path.join(*outfile.split('/')[:-1])}"
            self.chunkfolder = os.path.join(outfile_path, f'chunks_{outfile_name}')

    def compute(self):
        transpose_file(
            file=self.file,
            outfile=self.outfile,
            insep=self.insep,
            outsep=self.outsep,
            chunksize=self.chunksize,
            chunkfolder=self.chunkfolder,
            save_chunks=self.save_chunks,
            quiet=self.quiet,
        )
        
    def upload(self, 
        bucket: str,
        endpoint_url: str,
        aws_secret_key_id: str,
        aws_secret_access_key: str,
        remote_file_key: str=None,
        remote_chunk_path: str=None, 
    ) -> None:
        """
        Uploads the chunks and/or transposed file to the given S3 bucket.

        Parameters:
        bucket: Bucket name
        endpoint_url: S3 endpoint
        aws_secret_key_id: AWS secret key for your account 
        aws_secret_access_key: Specifies the secret key associated with the access key
        remote_file_key: Optional, key to upload file to in S3. Must be complete path, including file name 
        remote_chunk_path: Optional, key to upload chunks to in S3. Must be a folder-like path, where the chunks will be labeled as chunk_{outfile_name}_{l}.csv

        Returns:
        None
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
                Filename=self.outfile,
                Key=remote_file_key,
            )

        if remote_chunk_path:
            if not self.quiet and remote_chunk_path: print(f'Uploading chunks to {remote_chunk_path}')
            for file in os.listdir(self.chunkfolder):
                if not self.quiet: print(f'Uploading {file}')
                
                s3.Bucket(bucket).upload_file(
                    Filename=os.path.join(self.chunkfolder, file),
                    Key=os.path.join(remote_chunk_path, file)
                )
