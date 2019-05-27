##
# Offline Index
#
# @file
# @version 0.1
BUCKET=

clean:
	rm -Rf offline-index-cc/offline_worker_* && rm -Rf data/repo/* && rm -Rf .cluster_id

test: clean download
	sbt test

package:
	BUCKET=$(BUCKET) ./scripts/00_package.sh

cluster:
	BUCKET=$(BUCKET) ./scripts/01_create_cluster.sh

run:
	BUCKET=$(BUCKET) ./scripts/02_run_index.sh

download:
	./scripts/03_sync_test_data.sh

all: package cluster run

# end
