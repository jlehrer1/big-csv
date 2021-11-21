# Transpose csv
A Python script to transpose large csv/tsv files that can't fit in memory. This is designed to be run as a Kubernetes job. 

Suppose you have an $p \times m$ matrix where your original data is $m$ points samples with $p$ features, or in $m$ points in $p$ dimensional space. Then we want the column space to be the features, that is, we'd like to consider the $m \times p$ data matrix. This script does that calculation.

It works in the following way:
1. Read in chunks that fit in memory
2. Transpose those in memory (which is fast)
3. Write each transposed chunk to a `.np` file
4. Use `paste` to join the files horizontally (columnwise), this is why we don't need to save the index, since it will be the same as the columns of the original file. 

This process outputs the $m \times p$ matrix, as desired. 

To run, run `./run.sh`. Examine the `run.yaml` file to get started. The file name is passed in using `envsubst` which is defined as `FILE` in run.sh.