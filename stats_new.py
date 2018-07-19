import sys
import time
from pyspark import SparkContext, SparkConf

#Controllo di validita del record rappresentato da line
def is_valid(line):
	if len(line[0])==13 and len(line)==4:
		return 1
	else:
		return -1

#Funzione che associa una fascia per ogni valore
def statistics(num):
	if int(num)<86:
		return 85
	else:
		return int(num)

#Nome del file di output
today = time.strftime("%Y%m%d-%H%M%S")
#fileRisultato = "hdfs://localhost:9000/hduser/output/stats_new_" + today
fileRisultato = "hdfs://localhost:9000/user/gindi/output/stats_new_" + today

#Configurazione iniziale spark
conf=SparkConf().setAppName("Analisi statistiche")
sc=SparkContext(conf=conf)

#Unione dei files in input
rdd=sc.textFile("file:///home/gindi/spark-2.3.0-bin-hadoop2.7/bin/jars/Input/20180128/*/*/*/*/COM?????.NEW")
#rdd = sc.textFile("file:///usr/local/spark/input/20180128/*/*/*/*/COM?????.NEW")
text_file=rdd.coalesce(1).map(lambda line: line.split(";")).filter(lambda line: is_valid(line)==1)

#Creazione dell'output in rdd
stats_map=text_file.map(lambda line: (statistics(line[3]),1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x: x[0], False)

#Creazione del file di output
output=stats_map.saveAsTextFile(fileRisultato)
