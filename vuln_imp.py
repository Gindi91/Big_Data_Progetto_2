import sys
import time
from pyspark import SparkContext, SparkConf


#Funzione per ricavare il numero di ripetitori per ciascun contatore
def count_reps(line):
	return len(line)

def impact(line):
	return len(line)-1	

#Controllo di validita del record rappresentato da line
def is_valid(line):
	val=1
	for i in range(0, len(line)):
		if len(line[i])!=13:
			val=-1
	return val
	
#Nome del file di output
today = time.strftime("%Y%m%d-%H%M%S")
#fileRisultato = "hdfs://localhost:9000/hduser/output/imp_vuln_" + today
fileRisultato = "hdfs://localhost:9000/user/gindi/output/imp_vuln_" + today	

#Configurazione iniziale spark
conf=SparkConf().setAppName("")
sc=SparkContext(conf=conf)

#Unione dei files in input
rdd=sc.textFile("file:///home/gindi/spark-2.3.0-bin-hadoop2.7/bin/jars/Input/ROU/*.CFG")
#rdd = sc.textFile("file:///usr/local/spark/input/ROU/*.CFG")
text_file=rdd.coalesce(1).map(lambda line: line.split(";")).filter(lambda line: is_valid(line)==1)

#Calcolo della vulnerabilita per ogni contatore
vuln=text_file.map(lambda line: (line[0], count_reps(line))).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False)

#Calcolo dell'impatto totale per ogni contatore
imp=text_file.map(lambda line: (line[0], impact(line))).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False)

vuln_imp_join=vuln.join(imp, vuln[0]==imp[0]).map(lambda x,y,z: (x,y*z)).values()
vuln_imp_norm=float(vuln_imp_join.sum())/float(vuln_imp_join.count())

output = vuln_imp_norm.saveAsTextFile(fileRisultato)