#!/bin/sh

paste -d ',' chunks/* > ${1}.csv # Join all files horizontally
tail -n +2 ${1}.csv > ${1}_clipped.csv # Remove first line since it extraneous
mv ${1}_clipped.csv ${1}.csv # Move that fixed file back to the original passed file name 