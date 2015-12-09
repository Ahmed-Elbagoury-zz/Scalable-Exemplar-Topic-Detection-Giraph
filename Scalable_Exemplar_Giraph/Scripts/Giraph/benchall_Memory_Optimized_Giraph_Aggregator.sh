#!/bin/bash

# BEFORE RUNNING uncomment line 54 of org.apache.giraph.master.DeafultMasterCompute class. 
# Check settings in class VarianceAggregatorOptimized
# recompile Giraph (mvn compile)
source ../get-configs.sh

RUNS=1
Timeslots_Num=1
let "MACHINES=0" || true
while read slaveIP
do
let "MACHINES = $MACHINES + 1" || true
done < "$HADOOP_HOME"/conf/slaves

# WCC
for ((i=0; i<=Timeslots_Num; i++)); do
    for ((j = 1; j <= RUNS; j++)); do
        echo ./Exemplar_Aggregator_Optimized_AccumlateVariace.sh ${DATA_PATH}/$i ${MACHINES} $j $i
		./Exemplar_Aggregator_Optimized_AccumlateVariace.sh ${DATA_PATH}/$i ${MACHINES} $j $i
    done
done
exit
