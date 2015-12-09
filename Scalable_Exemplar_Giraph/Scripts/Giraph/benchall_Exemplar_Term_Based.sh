#!/bin/bash


source ../get-configs.sh

RUNS=3
Timeslots_Num=7
let "MACHINES=0" || true
while read slaveIP
do
let "MACHINES = $MACHINES + 1" || true
done < "$HADOOP_HOME"/conf/slaves

# WCC
for ((i=0; i<=Timeslots_Num; i++)); do
    for ((j = 1; j <= RUNS; j++)); do
		echo  ./Exemplar_Term_Based.sh ${DATA_PATH}/$i ${MACHINES} $j $i
        ./Exemplar_Term_Based.sh ${DATA_PATH}/$i ${MACHINES} $j $i
    done
done
exit
