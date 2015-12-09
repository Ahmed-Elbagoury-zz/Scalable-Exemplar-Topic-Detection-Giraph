#!/bin/bash -e

if [ $# -ne 4 ]; then
    echo "usage: $0 input-graph machines Run-Number Timeslot-Number"
    exit -1
fi

#source ../common/get-dirs.sh
source ../get-configs.sh

# place input in /user/${USER}/input/
# output is in /user/${USER}/giraph-output/
inputgraph=$1

# Technically this is the number of "workers", which can be more
# than the number of machines. However, using multiple workers per
# machine is inefficient! Use more Giraph threads instead (see below).
machines=$2
RunNum=$3
i=$4

## log names
logname=Exemplar_Aggregator_Optimized_${i}_${machines}_${TOPIC_NUM}_${RunNum}_"$(date +%Y%m%d-%H%M%S)"
logfile=${logname}_time.txt       # running time
#outputdir=/user/${USER}/giraph-output/${logname}/


#hadoop dfs -rmr "$outputdir" || true

## start logging memory + network usage
../bench-init.sh ${logname} giraph
hadoop dfs -rm /user/exp/ahmed/tweets-0 || true
hadoop dfs -rm /user/exp/ahmed/timeslot.txt || true
hadoop dfs -copyFromLocal ${TWEET_PATH}/tweets-${i} /user/exp/ahmed/tweets-0
T1=$(date +%s)
## start algorithm run
hadoop jar /home/exp/giraph-1.0.0/giraph-examples/target/giraph-examples-1.0.0-for-hadoop-0.20.203.0-jar-with-dependencies.jar  org.apache.giraph.GiraphRunner \
    -D mapred.child.java.opts=-Xmx${GIRAPH_MEMORY} -Dgiraph.zkSessionMsecTimeout=${GIRAPH_TIMEOUT} \
    -Dgiraph.numComputeThreads=${GIRAPH_THREADS} \
    -Dgiraph.numInputThreads=${GIRAPH_THREADS} \
    -Dgiraph.numOutputThreads=${GIRAPH_THREADS} \
	org.apache.giraph.examples.TopicVertexVocabBasedOptimizedAggregate \
    -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat \
    -vip ${inputgraph} \
    -ca TopicVertexVocabBasedOptimizedAggregate.source=2 \
    -w ${machines} 2>&1 | tee -a ~/logs/giraph/${logfile}


#-Dgiraph.numComputeThreads=${GIRAPH_THREADS} \
#    -Dgiraph.numInputThreads=${GIRAPH_THREADS} \
#    -Dgiraph.numOutputThreads=${GIRAPH_THREADS} \


	
T2=$(date +%s)
diffsec="$(expr $T2 - $T1)"
echo | awk -v D=$diffsec '{printf "Elapsed time: %02d:%02d:%02d\n",D/(60*60),D%(60*60)/60,D%60}' >> ${OUT_PATH}/${TOPIC_NUM}/${RunNum}/time.txt
## finish logging memory + network usage
../bench-finish.sh ${logname} giraph
hadoop dfs -cat /user/exp/ahmed/timeslot.txt > ${OUT_PATH}/${TOPIC_NUM}/${RunNum}/$i

/home/exp/experiments/kill-giraph-job.sh

## clean up step needed for Giraph
./kill-java-job.sh
