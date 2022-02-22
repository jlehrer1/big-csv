from .transpose import transpose_file

class Transpose:
    def __init__(
        self, 
        file: str, 
        outfile: str, 
        insep: str=',', 
        outsep: str=',',
        chunksize: str=400, 
        credentials_file: str=None,
        to_upload: bool=False, 
        save_chunks: bool=False,
        quiet: bool=False,
    ):
        self.file = file 
        self.outfile = outfile
        self.insep = insep 
        self.outsep = outsep
        self.chunksize = chunksize
        self.credentials_file = credentials_file
        self.to_upload = to_upload
        self.save_chunks = save_chunks
        self.quiet = quiet

    def compute(self):
        transpose_file(
            file=self.file,
            outfile=self.outfile,
            insep=self.insep,
            outsep=self.outsep,
            chunksize=self.chunksize,
            to_upload=self.to_upload,
            credentials_file=self.credentials_file,
            save_chunks=self.save_chunks,
            quiet=self.quiet,
        )
        