
DIR = $(shell readlink -f ${PWD})
#RUNTIME ?= apptainer
RUNTIME ?= singularity
EXTRA_ARGS = -B /lustre8/home/ddbjshare-pg:/lustre8/home/ddbjshare-pg
JQ = $(shell which jq)

all: help

clean:
	${RM} spark.sif

help:
	@echo "Usage: make <subcommands>"
	@echo "subcommands:"
	@echo "    build: build a container"
	@echo "    start: start Spark cluster (incl. starting containers)"
	@echo "    stop: stop Spark cluster (incl. stopping containers)"
	@echo "    status: show the current status of the cluster"

start: start-cluster
stop: stop-containers

.SUFFIXES: .def .sif

build: spark.sif

%.sif: %.def
	${RUNTIME} build --fakeroot $@ $^

start-containers:
	@test -e workers || (echo 'File `workers` not found. See `workers.sample` for preparation' && false)
	@mkdir -p ${DIR}/log ${DIR}/work
	@cat workers | grep -v "^#" | xargs -n 1 -i ssh ${USER}@{} ${RUNTIME} instance start \
	  -B $(mktemp -d ${DIR}/run/$(hostname)_XXXX):/run \
	  -B ${DIR}/log/:/usr/local/spark/logs \
	  -B ${DIR}/work/:/usr/local/spark/work \
	  -B ${DIR}/workers:/usr/local/spark/conf/workers \
	  ${EXTRA_ARGS} \
	  ${DIR}/spark.sif spark

start-cluster: start-containers
	@${RUNTIME} exec instance://spark start-all.sh

stop-containers: stop-cluster
	@cat workers | grep -v "^#" | xargs -n 1 -i ssh ${USER}@{} ${RUNTIME} instance stop spark

stop-cluster:
	@${RUNTIME} exec instance://spark stop-all.sh

status:
	@echo -e "HOST\tPID\tINSTANCE_NAME"
	@cat workers | grep -v "^#" | \
	  xargs -n 1 -i ssh ${USER}@{} '${RUNTIME} instance list -j | ${JQ} ".instances[0] | .host = \"{}\""' | \
	  ${JQ} -r '.host + " " + (.pid | tostring) + " " + .instance'
	@echo ""
	@echo "You can see the cluster info at http://localhost:8080/ if available"
