import sys
import time
from pyspark import SparkContext, SparkConf

#Parametro inserito da riga di comando, contiene il nome del file da elaborare.
FileInput =  sys.argv[1]
pathFile = "hdfs://localhost:9000/user/gindi/input/" + FileInput
#pathFile = "hdfs://localhost:9000/hduser/input/" + FileInput

#Controllo di validita del record rappresentato da line
def is_valid(line):
	if '/' not in line[2] and '"' not in line[2]:
		return 1
	else:
		return -1

#Funzione per il calcolo dell'impatto del singolo contatore in un record
def impact(line):
	if line[3] is None or line[3]=="               ":
		return 1 
	elif line[4] is None  or line[4]=="               ":
		return 2
	elif line[5] is None  or line[5]=="               ":
		return 3
	else:
		return 4

#Nome del file di output
today = time.strftime("%Y%m%d-%H%M%S")
#fileRisultato = "hdfs://localhost:9000/hduser/output/imp_" + today + ".txt"	
fileRisultato = "hdfs://localhost:9000/user/gindi/output/imp_" + today + ".txt"

#Configurazione iniziale spark
conf=SparkConf().setAppName("Misurazione dell'impatto")
sc=SparkContext(conf=conf)
text_file=sc.textFile(pathFile).map(lambda line: line.split(";")).filter(lambda line: line[2]!="RIPETITORE1").filter(lambda line: is_valid(line)==1)

#Calcolo dell'impatto totale dei vari contatori
imp=text_file.map(lambda line: (line[2], impact(line))).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False)

#Calcolo della media d'impatto nell'intera rete
mean_map=text_file.map(lambda line: (1, impact(line))).values()
mean_imp=sc.parallelize(["Media totale d'impatto nella rete",float(mean_map.sum())/float(mean_map.count())])

#Unione dei due risultati parziali e scrittura dei file su HDFS
output = sc.union([mean_imp,imp]).saveAsTextFile(fileRisultato)
