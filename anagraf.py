import sys
import time
from pyspark import SparkContext, SparkConf

#Controllo di validita del record rappresentato da line
def is_valid(line):
	if len(line)==5 or len(line)==7:
		return 1
	else:
		return -1

#Nome del file di output
today = time.strftime("%Y%m%d-%H%M%S")
#fileRisultato = "hdfs://localhost:9000/hduser/output/anagraf_" + today
fileRisultato = "hdfs://localhost:9000/user/gindi/output/anagraf_" + today	

#Configurazione iniziale spark
conf=SparkConf().setAppName("Elaborazione dati anagrafici")
sc=SparkContext(conf=conf)

#Unione dei files in input
rdd=sc.textFile("file:///home/gindi/spark-2.3.0-bin-hadoop2.7/bin/jars/Input/*/*/*/*/ADD?????.CFG")
#rdd = sc.textFile("file:///usr/local/spark/input/*/*/*/*/ADD?????.CFG")
text_file=rdd.coalesce(1).map(lambda line: line.split(";")).filter(lambda line: is_valid(line)==1)

#Calcolo del numero di contatori per fase
phase_map=text_file.map(lambda line: (line[3],1)).reduceByKey(lambda x,y: x+y)

#Creazione del file di output
output=phase_map.saveAsTextFile(fileRisultato)
