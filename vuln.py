import sys
import time
from pyspark import SparkContext, SparkConf

#Funzione per ricavare il numero di ripetitori per ciascun contatore
def count_reps(line):
	return len(line)

#Controllo di validita del record rappresentato da line
def is_valid(line):
	val=1
	for i in range(0, len(line)):
		if len(line[i])!=13:
			val=-1
	return val

#Nome del file di output
today = time.strftime("%Y%m%d-%H%M%S")
#fileRisultato = "hdfs://localhost:9000/hduser/output/vuln_" + today
fileRisultato = "hdfs://localhost:9000/user/gindi/output/vuln_" + today	
	
#Configurazione iniziale spark
conf=SparkConf().setAppName("Controllo delle vulnerabilita")
sc=SparkContext(conf=conf)

#Unione dei files in input
#rdd=sc.textFile("file:///home/gindi/spark-2.3.0-bin-hadoop2.7/bin/jars/Input/20180128/*/*/*/*/ROU?????.CFG")
rdd = sc.textFile("file:///usr/local/spark/input/20180128/*/*/*/*/ROU?????.CFG")
text_file=rdd.coalesce(1).map(lambda line: line.split(";")).filter(lambda line: is_valid(line)==1)

#Calcolo del numero di contatori per livello di vulnerabilita
vuln=text_file.map(lambda line: (count_reps(line),1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[0], False)

#Calcolo della vulnerabilita totale media del sistema
vuln_map=text_file.map(lambda line: (1, count_reps(line))).values()
mean_vuln=sc.parallelize(["Vulnerabilita totale media del sistema:",float(vuln_map.sum())/float(vuln_map.count())])

#Unione dei due risultati parziali e scrittura dei file su HDFS
output = sc.union([mean_vuln,vuln]).saveAsTextFile(fileRisultato)
