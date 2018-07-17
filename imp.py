import sys
from pyspark import SparkContext, SparkConf

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

#Configurazione iniziale spark
conf=SparkConf().setAppName("Misurazione dell'impatto")
sc=SparkContext(conf=conf)
text_file=sc.textFile("hdfs://localhost:9000/user/gindi/input/file_rou_20180311_mini.txt").map(lambda line: line.split(";")).filter(lambda line: is_valid(line)==1)

#Calcolo dell'impatto totale dei vari contatori
impact_list=text_file.map(lambda line: (line[2], impact(line))).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False).collect()

#Calcolo della media d'impatto nell'intera rete
mean_map=text_file.map(lambda line: (1, impact(line))).values()
mean=float(mean_map.sum())/float(mean_map.count())

#Rappresentazione dei risultati
print "=========================="
print "Media totale d'impatto nella rete"
print mean
print "=========================="
print "Contatori di maggior impatto"
for record in impact_list:
	print record
print "=========================="
