logname=Center_SuperTuesday_100
../bench-init.sh ${logname} giraph
T1=$(date +%s)
java  -Xmx12000m -jar exemplar_center.jar /home/exp/IEEETrans_SuperTuesday/SuperTuesday_selected /home/exp/IEEETrans/stopwords.txt /home/exp/Giraph/Center_SuperTuesday/100 100
T2=$(date +%s)
diffsec="$(expr $T2 - $T1)"
echo | awk -v D=$diffsec '{printf "Elapsed time: %02d:%02d:%02d\n",D/(60*60),D%(60*60)/60,D%60}' >> /home/exp/Giraph/Center_SuperTuesday/100/time.txt
../bench-finish.sh ${logname} giraph

logname=Center_SuperTuesday_50
../bench-init.sh ${logname} giraph
T1=$(date +%s)
java  -Xmx12000m -jar exemplar_center.jar /home/exp/IEEETrans_SuperTuesday/SuperTuesday_selected /home/exp/IEEETrans/stopwords.txt /home/exp/Giraph/Center_SuperTuesday/50 50
T2=$(date +%s)
diffsec="$(expr $T2 - $T1)"
echo | awk -v D=$diffsec '{printf "Elapsed time: %02d:%02d:%02d\n",D/(60*60),D%(60*60)/60,D%60}' >> /home/exp/Giraph/Center_SuperTuesday/50/time.txt
../bench-finish.sh ${logname} giraph

logname=Center_SuperTuesday_10
../bench-init.sh ${logname} giraph
T1=$(date +%s)
java  -Xmx12000m -jar exemplar_center.jar /home/exp/IEEETrans_SuperTuesday/SuperTuesday_selected /home/exp/IEEETrans/stopwords.txt /home/exp/Giraph/Center_SuperTuesday/10 10
T2=$(date +%s)
diffsec="$(expr $T2 - $T1)"
echo | awk -v D=$diffsec '{printf "Elapsed time: %02d:%02d:%02d\n",D/(60*60),D%(60*60)/60,D%60}' >> /home/exp/Giraph/Center_SuperTuesday/10/time.txt
../bench-finish.sh ${logname} giraph

logname=Center_USElection_100
../bench-init.sh ${logname} giraph
T1=$(date +%s)
java  -Xmx12000m -jar exemplar_center.jar /home/exp/IEEETrans/USElections_selected /home/exp/IEEETrans/stopwords.txt /home/exp/Giraph/Center_US_Election/100 100
T2=$(date +%s)
diffsec="$(expr $T2 - $T1)"
echo | awk -v D=$diffsec '{printf "Elapsed time: %02d:%02d:%02d\n",D/(60*60),D%(60*60)/60,D%60}' >> /home/exp/Giraph/Center_US_Election/100/time.txt
../bench-finish.sh ${logname} giraph

logname=Center_USElection_50
../bench-init.sh ${logname} giraph
T1=$(date +%s)
java  -Xmx12000m -jar exemplar_center.jar /home/exp/IEEETrans/USElections_selected /home/exp/IEEETrans/stopwords.txt /home/exp/Giraph/Center_US_Election/50 50
T2=$(date +%s)
diffsec="$(expr $T2 - $T1)"
echo | awk -v D=$diffsec '{printf "Elapsed time: %02d:%02d:%02d\n",D/(60*60),D%(60*60)/60,D%60}' >> /home/exp/Giraph/Center_US_Election/50/time.txt
../bench-finish.sh ${logname} giraph

logname=Center_USElection_10
../bench-init.sh ${logname} giraph
T1=$(date +%s)
java  -Xmx12000m -jar exemplar_center.jar /home/exp/IEEETrans/USElections_selected /home/exp/IEEETrans/stopwords.txt /home/exp/Giraph/Center_US_Election/10 10
T2=$(date +%s)
diffsec="$(expr $T2 - $T1)"
echo | awk -v D=$diffsec '{printf "Elapsed time: %02d:%02d:%02d\n",D/(60*60),D%(60*60)/60,D%60}' >> /home/exp/Giraph/Center_US_Election/10/time.txt
../bench-finish.sh ${logname} giraph




