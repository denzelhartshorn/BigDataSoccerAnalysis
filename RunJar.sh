hadoop fs -rm -r /Final/output
hadoop fs -rm -r /Final/DataReducedOutput
# hadoop fs -mkdir /Final/DataReducedOutput

CreateJar.sh

# hadoop fs
hadoop jar ~/Desktop/CS435/Final/DataMapReduce/DataMapReduce.jar datamapreduce.DataMapReduce /Final/dataset /Final/output
