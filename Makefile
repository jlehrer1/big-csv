CONTAINER = jmlehrer/transpose

exec:
	docker exec -it $(CONTAINER) /bin/bash

build:
	docker build -t $(CONTAINER) .

push:
	docker push $(CONTAINER)

run:
	python transpose.py -file organoid.tsv -chunksize=200 -sep='\t'

all:
	docker build -t $(CONTAINER) . && docker push $(CONTAINER) && ./run.sh

clean:
	kubectl delete job transpose-organoid && kubectl delete job transpose-primary
