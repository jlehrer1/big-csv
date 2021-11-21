import boto3
import pathlib 
import os 
import argparse

# Defines upload function and uploades combined data after all chunks are generated
s3 = boto3.resource(
    's3',
    endpoint_url="https://s3.nautilus.optiputer.net",
    aws_access_key_id="EFIE1S59OR5CHDC4KCHK",
    aws_secret_access_key="DRXgeKsTLctfFX9udqfT04go8JpxG3qWxj0OKHVU",
)
   
def upload(file_name, remote_name=None):
    if remote_name == None:
        remote_name = file_name

    s3.Bucket('braingeneersdev').upload_file(
        Filename=file_name,
        Key=os.path.join('jlehrer', 'transposed_data', remote_name)
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Upload the file passed in the -file argument. Also a helper file that defines our upload function'
    )

    parser.add_argument('-file', metavar='F', type=str, help='Path to the input file')
    args = parser.parse_args()
    file = args.file

    here = pathlib.Path(__file__).parent.resolve()
    upload(os.path.join(here, f'{file}'), f'{file}')