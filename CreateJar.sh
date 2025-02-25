hadoop com.sun.tools.javac.Main *.java

mv *.class datamapreduce

jar cf DataMapReduce.jar datamapreduce/
