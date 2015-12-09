#!/bin/bash -e

if [ $# -ne 3 ]; then
    echo "usage: $0 input-graph machines edge-type"
    echo ""
    echo "edge-type: 0 for byte array edges"
    echo "           1 for hash map edges"
    exit -1
fi

#source ../common/get-dirs.sh
source ../get-configs.sh

# place input in /user/${USER}/input/
# output is in /user/${USER}/giraph-output/
inputgraph=$(basename $1)

echo $inputgraph

# Technically this is the number of "workers", which can be more
# than the number of machines. However, using multiple workers per
# machine is inefficient! Use more Giraph threads instead (see below).
machines=$2

edgetype=$3
case ${edgetype} in
    0) edgeclass="";;     # byte array edges are used by default
    1) edgeclass="-Dgiraph.inputOutEdgesClass=org.apache.giraph.edge.HashMapEdges \
                  -Dgiraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges";;
    *) echo "Invalid edge-type"; exit -1;;
esac

## log names
logname=Exemplar_SlidingWindows_${inputgraph}_${machines}_${edgetype}_"$(date +%Y%m%d-%H%M%S)"
logfile=${logname}_time.txt       # running time
#outputdir=/user/${USER}/giraph-output/${logname}/


#hadoop dfs -rmr "$outputdir" || true

## start logging memory + network usage
../bench-init.sh ${logname} giraph
T1=$(date +%s)

## start algorithm run
hadoop jar "$GIRAPH_HOME"/giraph-examples/target/giraph-examples-1.0.0-for-hadoop-1.0.2-jar-with-dependencies.jar org.apache.giraph.GiraphRunner \
    -D mapred.child.java.opts=-Xmx15000M -Dgiraph.zkSessionMsecTimeout=900000 \
	org.apache.giraph.examples.TopicVertexVocabBasedAggregateDynamic \
    -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat \
    -vip ${inputgraph} \
    -ca TopicVertexVocabBasedAggregateDynamic.source=2 \
    -w ${machines} 2>&1 | tee -a ~/logs/giraph/${logfile}

	
T2=$(date +%s)
diffsec="$(expr $T2 - $T1)"
echo | awk -v D=$diffsec '{printf "Elapsed time: %02d:%02d:%02d\n",D/(60*60),D%(60*60)/60,D%60}' >> ${TIME_FILE}
## finish logging memory + network usage
../bench-finish.sh ${logname} giraph

## clean up step needed for Giraph
./kill-java-job.sh
