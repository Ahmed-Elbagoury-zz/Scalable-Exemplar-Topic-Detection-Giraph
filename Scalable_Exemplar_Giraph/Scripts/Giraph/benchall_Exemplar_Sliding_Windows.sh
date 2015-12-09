#!/bin/bash


source ../get-configs.sh

RUNS=1
Timeslots_Num=25
let "MACHINES=0" || true
while read slaveIP
do
let "MACHINES = $MACHINES + 1" || true
done < "$HADOOP_HOME"/conf/slaves

echo $RUNS
echo $MACHINES

# WCC
for i in {0..Timeslots_Num}; do
    for ((j = 1; j <= RUNS; j++)); do
        ./Exemplar_Sliding_Windows.sh ${DATA_PATH}/$i ${MACHINES}
    done
done
exit
