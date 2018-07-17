import sys
from pyspark import SparkContext, SparkConf

conf=SparkConf().setAppName("Controllo delle vulnerabilita")
sc=SparkContext(conf=conf)

result=sc.textFile("hdfs://localhost:9000/user/gindi/input/file_rou_20180311_mini.txt").map(lambda line: line.split(";")).filter(lambda line: len(line[6])==1).map(lambda line: (line[6],1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[0], False).collect()

print "=========================="
print result
print "=========================="
