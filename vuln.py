import sys
import time
from pyspark import SparkContext, SparkConf

#Parametro inserito da riga di comando, contiene il nome del file da elaborare.
FileInput =  sys.argv[1]
# pathFile = "hdfs://localhost:9000/user/gindi/input/" + FileInput
pathFile = "hdfs://localhost:9000/hduser/input/" + FileInput

#Funzione per ricavare il numero di ripetitori per ciascun contatore
def count_reps(line):
	if line[3] is None or line[3]=="               ":
		return 2
	elif line[4] is None  or line[4]=="               ":
		return 3
	elif line[5] is None  or line[5]=="               ":
		return 4
	else:
		return 5

#Controllo di validita del record rappresentato da line
def is_valid(line):
	if '/' not in line[2] and '"' not in line[2]:
		return 1
	else:
		return -1

#Nome del file di output
today = time.strftime("%Y%m%d-%H%M%S")
fileRisultato = "hdfs://localhost:9000/hduser/output/vuln_" + today + ".txt"	
	
#Configurazione iniziale spark
conf=SparkConf().setAppName("Controllo delle vulnerabilita")
sc=SparkContext(conf=conf)
text_file=sc.textFile(pathFile).map(lambda line: line.split(";")).filter(lambda line: line[2]!="RIPETITORE1").filter(lambda line: is_valid(line)==1)

#Calcolo del numero di contatori per livello di vulnerabilita
vuln=text_file.map(lambda line: (count_reps(line),1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[0], False)

#Calcolo della vulnerabilita totale media del sistema
vuln_map=text_file.map(lambda line: (1, count_reps(line))).values()
mean_vuln=sc.parallelize(["Vulnerabilita totale media del sistema:",float(vuln_map.sum())/float(vuln_map.count())])

#Unione dei due risultati parziali e scrittura dei file su HDFS
output = sc.union([mean_vuln,vuln]).saveAsTextFile(fileRisultato)
