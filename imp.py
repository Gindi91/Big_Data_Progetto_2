import sys
import time
from pyspark import SparkContext, SparkConf

#Controllo di validita del record rappresentato da line
def is_valid(line):
	val=1
	for i in range(0, len(line)):
		if len(line[i])!=13:
			val=-1
	return val

#Funzione per il calcolo dell'impatto del singolo contatore in un record
def impact(line):
	return len(line)-1

#Nome del file di output
today = time.strftime("%Y%m%d-%H%M%S")
#fileRisultato = "hdfs://localhost:9000/hduser/output/imp_" + today + ".txt"	
fileRisultato = "hdfs://localhost:9000/user/gindi/output/imp_" + today

#Configurazione iniziale spark
conf=SparkConf().setAppName("Misurazione dell'impatto")
sc=SparkContext(conf=conf)

#Unione dei files in input
rdd=sc.textFile("file:///home/gindi/spark-2.3.0-bin-hadoop2.7/bin/jars/Input/20180128/*/*/*/*/ROU?????.CFG")
#rdd = sc.textFile("file:///usr/local/spark/input/20180128/*/*/*/*/ROU?????.CFG")
text_file=rdd.coalesce(1).map(lambda line: line.split(";")).filter(lambda line: is_valid(line)==1)

#Calcolo dell'impatto totale dei vari contatori
imp=text_file.map(lambda line: (line[0], impact(line))).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False)

#Calcolo della media d'impatto nell'intera rete
mean_map=text_file.map(lambda line: (1, impact(line))).values()
mean_imp=sc.parallelize(["Media totale d'impatto nella rete",float(mean_map.sum())/float(mean_map.count())])

#Creazione del log dei record non validi
not_valid=rdd.coalesce(1).map(lambda line: line.split(";")).filter(lambda line: is_valid(line)==-1)

#Unione dei risultati parziali e scrittura dei file su HDFS
output = sc.union([mean_imp,imp,not_valid]).saveAsTextFile(fileRisultato)
