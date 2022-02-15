from os import sep
from transpose import transpose_file

class Transpose:
    def __init__(
        self, 
        file: str, 
        write_path: str, 
        sep: str=',', 
        chunksize: str=400, 
        credentials_file: str=None, 
        to_upload: bool=False, 
    ):
        self.file = file 
        self.write_path = write_path
        self.sep = sep 
        self.chunksize = chunksize
        self.credentials_file = credentials_file
        self.to_upload = to_upload

    def compute(self):
        transpose_file(
            file=self.file,
            write_path=self.write_path,
            sep=self.sep,
            chunksize=self.chunksize,
            to_upload=self.to_upload 
        )
        