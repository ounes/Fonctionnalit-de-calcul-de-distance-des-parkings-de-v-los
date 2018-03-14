from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark import SparkContext
from pyspark.sql import Row
import sys, re

sc = SparkContext(master='local[2]', appName='MeteoDf')
spark = SparkSession\
    .builder.appName("MeteoDf")\
    .config("master", "local[2]")\
    .getOrCreate()

lines = sc.textFile(sys.argv[1])

def test_word(word):
    if ('@' not in word and '/' not in word):
        return word

def add(a,b):
    return a+b

Word = Row('word', 'freq', 'longueur')

lines_map = lines.map(lambda line: line.strip(' '))\
        .flatMap(lambda line: line.split(' '))\
        .map(lambda word: word.strip('()`,.;:?!\"-\'*'))\
        .filter(lambda word: '@' not in word and '/' not in word and word!='')\
        .map(lambda word: test_word(word))\
        .map(lambda word: (str(word).lower(),1))\
        .reduceByKey(add)\
        .map(lambda (word,freq): (word,freq,len(word)))\
        .map(lambda word: Word(*word))\
        .collect()

df = spark.createDataFrame(lines_map)
#df.createTempView("df_cache")
#spark.sql("select * from df_cache order by longueur desc").show()
df.orderBy(df.longueur, ascending=False).show()

#spark.sql("select * from df_cache where longueur=4 order by freq desc").show()
df.filter(df.longueur == 4)\
.orderBy(df.freq, ascending=False)\
.show()

#spark.sql("select * from df_cache where longueur=15 order by freq desc").show()
df.filter(df.longueur == 15)\
.orderBy(df.freq, ascending=False)\
.show()