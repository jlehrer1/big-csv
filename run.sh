#!/bin/bash

export FILE=organoid
envsubst < run.yaml | kubectl create -f -

export FILE=primary
envsubst < run.yaml | kubectl create -f -

