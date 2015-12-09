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
logname=Exemplar_Msg_Optimized_${i}_${machines}_${TOPIC_NUM}_${RunNum}_"$(date +%Y%m%d-%H%M%S)"
logfile=${logname}_time.txt       # running time
#outputdir=/user/${USER}/giraph-output/${logname}/


#hadoop dfs -rmr "$outputdir" || true
hadoop dfs -rmr /user/exp/ahmed/topic_detection_us_election || true
## start logging memory + network usage
../bench-init.sh ${logname} giraph
T1=$(date +%s)
## start algorithm run

	
hadoop jar /home/exp/giraph-1.0.0/giraph-examples/target/giraph-examples-1.0.0-for-hadoop-0.20.203.0-jar-with-dependencies.jar  org.apache.giraph.GiraphRunner \
	-D mapred.child.java.opts=-Xmx${GIRAPH_MEMORY} -Dgiraph.zkSessionMsecTimeout=${GIRAPH_TIMEOUT} \
	org.apache.giraph.examples.TopicVertexShared \
	-vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat \
	-vip ${inputgraph} -of org.apache.giraph.examples.IdWithValueConflictListTextOutputFormat \
	-op /user/exp/ahmed/topic_detection_us_election  \
	-ca TopicVertexShared.source=2 -w ${machines} 2>&1 | tee -a ~/logs/giraph/${logfile}

java -jar /home/exp/hadoop-1.0.4/exemplar_HDFS.jar /user/exp/ahmed/topic_detection_us_election ${TOPIC_NUM} ${TWEET_PATH}/tweets-${i} ${OUT_PATH}/${TOPIC_NUM}/${RunNum}/${i}

T2=$(date +%s)
diffsec="$(expr $T2 - $T1)"
echo | awk -v D=$diffsec '{printf "Elapsed time: %02d:%02d:%02d\n",D/(60*60),D%(60*60)/60,D%60}' >> ${OUT_PATH}/${TOPIC_NUM}/${RunNum}/time.txt

## finish logging memory + network usage
../bench-finish.sh ${logname} giraph
#sleep 5m
/home/exp/experiments/kill-giraph-job.sh

## clean up step needed for Giraph
./kill-java-job.sh
