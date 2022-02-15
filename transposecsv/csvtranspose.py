from .transpose import transpose_file

class Transpose:
    def __init__(
        self, 
        file: str, 
        outfile: str, 
        sep: str=',', 
        chunksize: str=400, 
        credentials_file: str=None,
        to_upload: bool=False, 
        quiet: bool=False,
    ):
        self.file = file 
        self.outfile = outfile
        self.sep = sep 
        self.chunksize = chunksize
        self.credentials_file = credentials_file
        self.to_upload = to_upload
        self.quiet = quiet

    def compute(self):
        transpose_file(
            file=self.file,
            outfile=self.outfile,
            sep=self.sep,
            chunksize=self.chunksize,
            to_upload=self.to_upload,
            credentials_file=self.credentials_file,
            quiet=self.quiet
        )
        