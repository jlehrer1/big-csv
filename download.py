import urllib.request

# urls and their file names locally go here
urls = {
    "https://cells.ucsc.edu/organoidreportcard/organoids10X/exprMatrix.tsv.gz" : "organoid.tsv.gz",
    "https://cells.ucsc.edu/organoidreportcard/primary10X/exprMatrix.tsv.gz" : "primary.tsv.gz"
}

for url, file in urls.items():
    print(f'Downloading {file}')
    urllib.request.urlretrieve(url, file)