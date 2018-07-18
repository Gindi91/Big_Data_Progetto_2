import sys
import time
from pyspark import SparkContext, SparkConf

#Controllo di validita del record rappresentato da line
def is_valid(line):
	if len(line)==2:
		return 1
	else:
		return -1

#Funzione che estrae il valore numerico
def ex_num(line):
	return double(line[1][:-2])

#Funzione che associa una fascia per ogni valore
def statistics(num):
	if num<86:
		return 85
	else return int(num)
	

#Nome del file di output
today = time.strftime("%Y%m%d-%H%M%S")
#fileRisultato = "hdfs://localhost:9000/hduser/output/stats_" + today
fileRisultato = "hdfs://localhost:9000/user/gindi/output/stats_" + today

#Configurazione iniziale spark
conf=SparkConf().setAppName("")
sc=SparkContext(conf=conf)

#Unione dei files in input
rdd=sc.textFile("file:///home/gindi/spark-2.3.0-bin-hadoop2.7/bin/jars/Input/COM/*.txt")
#rdd = sc.textFile("file:///usr/local/spark/input/COM/*.txt")
text_file=rdd.coalesce(1).map(lambda line: line.split("(")).filter(lambda line: is_valid(line)==1)

stats_map=text_file.map(lambda line: (statistics(ex_num(line)),1))

	

