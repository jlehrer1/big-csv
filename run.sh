FILE=organoid
envsubst < run.yaml | kubectl create -f -

FILE=primary
envsubst < run.yaml | kubectl create -f -
