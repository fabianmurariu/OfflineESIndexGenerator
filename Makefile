##
# Offline Index
#
# @file
# @version 0.1

clean:
	rm -Rf offline-index-cc/offline_worker_* && rm -Rf data/repo/* && rm -Rf .cluster_id

test: clean
	sbt test

package:
	./scripts/00_package.sh

cluster:
	./scripts/01_create_cluster.sh

run:
	./scripts/02_run_index.sh

all: package cluster run

# end
