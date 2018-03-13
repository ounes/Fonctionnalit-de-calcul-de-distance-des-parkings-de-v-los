from pyspark import SparkContext
import sys

sc = SparkContext(master='local[2]', appName='Meteo')
lines = sc.textFile(sys.argv[1])

lines_map = lines.map(lambda line: (line[19:21], int(line[87:92]), int(line[92])))\
.filter(lambda (mois,temp,qlty): (temp!=9999 and qlty in (0,1,4,5,9)))\
.map(lambda tup: (tup[0],float(tup[1])/10))\
.reduceByKey(max)\
.sortByKey()\
.saveAsTextFile('hdfs://localhost:9000/user/diginamic/resultatsMeteo')

# Requete sur le shell: 
# "spark-submit meteo.py hdfs://localhost:9000/user/diginamic/input/meteo"