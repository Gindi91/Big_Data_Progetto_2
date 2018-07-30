import sys
import time
from pyspark import SparkContext, SparkConf

#Controllo di validita per i campi stats
def is_valid_stats(line):
	if len(line)==2 and (len(line[1])==7 or len(line[1])==8):
		return 1
	else:
		return -1

#Controllo di validita per i campi imp
def is_valid_imp(line):
	val=1
	for i in range(0, len(line)):
		if len(line[i])!=13:
			val=-1
	return val

#Funzione per il calcolo dell'impatto del singolo contatore in un record
def impact(line):
	return len(line)-1

#Funzione che estrae il valore numerico
def ex_num(line):
	return int(float(line[1][:-2]))

def get_contatore(line):
	return line[0][0:13]

#Nome del file di output
today = time.strftime("%Y%m%d-%H%M%S")
#fileRisultato = "hdfs://localhost:9000/hduser/output/stats_" + today
fileRisultato = "hdfs://localhost:9000/user/gindi/output/stats_imp_" + today

#Configurazione iniziale spark
conf=SparkConf().setAppName("")
sc=SparkContext(conf=conf)

#Unione dei files in input
rdd_imp=sc.textFile("file:///home/gindi/spark-2.3.0-bin-hadoop2.7/bin/jars/Input/ROU/*.CFG")
text_file_imp=rdd_imp.coalesce(1).map(lambda line: line.split(";")).filter(lambda line: is_valid_imp(line)==1)

rdd_stats=sc.textFile("file:///home/gindi/spark-2.3.0-bin-hadoop2.7/bin/jars/Input/COM/*.TXT")
#rdd = sc.textFile("file:///usr/local/spark/input/COM/*.TXT")
text_file_stats=rdd_stats.coalesce(1).map(lambda line: line.split("(")).filter(lambda line: is_valid_stats(line)==1)

#Calcolo dell'impatto totale per ogni contatore
imp=text_file_imp.map(lambda line: (line[0], impact(line))).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False)

#Calcolo della statistica relativa ad ogni contatore
stats=text_file_stats.map(lambda line: (get_contatore(line), ex_num(line))).sortBy(lambda x: x[1], False)

#Calcolo del prodotto normalizzato
stats_imp_join=stats.join(imp).coalesce(1).values().map(lambda x: x[0]*x[1])
stats_imp_norm=float(stats_imp_join.sum())/float(stats_imp_join.count())

#Creazione del log dei record non validi
not_valid_imp=rdd_imp.coalesce(1).map(lambda line: line.split(";")).filter(lambda line: is_valid_imp(line)==-1)
sc.parallelize(["Record non validi in ROU", not_valid_imp]

not_valid_stats=rdd_stats.coalesce(1).map(lambda line: line.split("(")).filter(lambda line: is_valid_stats(line)==-1)
sc.parallelize(["Record non validi in COM", not_valid_imp]

output = sc.union([sc.parallelize(["Prodotto normalizzato tra statistica ed impatto della rete", stats_imp_norm]),not_valid_imp,not_valid_stats]).saveAsTextFile(fileRisultato)
