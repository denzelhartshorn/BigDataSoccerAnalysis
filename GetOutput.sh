rm -r ../Data/Output
rm -r ./output
mkdir ../Data/Output
mkdir ./output

hadoop fs -get /Final/DataReducedOutput ~/Desktop/CS435/Final/Data/Output
hadoop fs -get /Final/output ~/Desktop/CS435/Final/DataMapReduce/output