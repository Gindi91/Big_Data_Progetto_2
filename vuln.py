import sys
from pyspark import SparkContext, SparkConf

#Funzione per ricavare il numero di ripetitori per ciascun contatore
def count_reps(line):
	if line[3] is None or line[3]=="               ":
		return 1
	elif line[4] is None  or line[4]=="               ":
		return 2
	elif line[5] is None  or line[5]=="               ":
		return 3
	else:
		return 4

#Controllo di validita del record rappresentato da line
def is_valid(line):
	if '/' not in line[2] and '"' not in line[2]:
		return 1
	else:
		return -1

#Configurazione iniziale spark
conf=SparkConf().setAppName("Controllo delle vulnerabilita")
sc=SparkContext(conf=conf)

#Calcolo e rappresentazione risultato
result=sc.textFile("hdfs://localhost:9000/user/gindi/input/file_rou_20180311_mini.txt").map(lambda line: line.split(";")).filter(lambda line: is_valid(line)==1).map(lambda line: (count_reps(line),1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[0], False).collect()


print "=========================="
print result
print "=========================="
