import sys
import time
from pyspark import SparkContext, SparkConf

#Controllo di validita del record rappresentato da line
def is_valid(line):
	if len(line)==2 and (len(line[1])==7 or len(line[1])==8):
		return 1
	else:
		return -1

#Funzione che estrae il valore numerico
def ex_num(line):
	return float(line[1][:-2])

#Funzione che associa una fascia per ogni valore
def statistics(num):
	if num<86:
		return 85
	else:
		return int(num)

#Nome del file di output
today = time.strftime("%Y%m%d-%H%M%S")
#fileRisultato = "hdfs://localhost:9000/hduser/output/stats_" + today
fileRisultato = "hdfs://localhost:9000/user/gindi/output/stats_" + today

#Configurazione iniziale spark
conf=SparkConf().setAppName("Analisi statistiche")
sc=SparkContext(conf=conf)

#Unione dei files in input
rdd=sc.textFile("file:///home/gindi/spark-2.3.0-bin-hadoop2.7/bin/jars/Input/COM/*.TXT")
#rdd = sc.textFile("file:///usr/local/spark/input/COM/*.TXT")
text_file=rdd.coalesce(1).map(lambda line: line.split("(")).filter(lambda line: is_valid(line)==1)

#Creazione dell'output in rdd
stats_map=text_file.map(lambda line: (statistics(ex_num(line)),1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x: x[0], False)

#Creazione del log dei record non validi
not_valid=rdd.coalesce(1).map(lambda line: line.split("(")).filter(lambda line: is_valid(line)==-1)

#Creazione del file di output
output = sc.union([stats_map,not_valid]).saveAsTextFile(fileRisultato)
