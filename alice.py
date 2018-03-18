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

def filter_stop_words(words):
    from nltk.corpus import stopwords
    #from nltk import download
    #nltk.download("stopwords")
    english_stop_words = set(stopwords.words("english"))
    for word in words:
        if word not in english_stop_words:
            yield word
    
def add(a,b):
    return a+b

'''nbMots = sc.accumulator(0)
def customAcc(word):
    nbMots.add(1)
    return (word,1)'''

Word = Row('word', 'freq', 'longueur')

lines_traitement = lines.map(lambda line: line.strip(' '))\
        .flatMap(lambda line: line.split(' '))\
        .map(lambda word: word.strip('()`,.;:?!\"-\'*'))\
        .filter(lambda word: '@' not in word and '/' not in word and word!='')\
        .map(lambda word: str(word).lower())\
        .mapPartitions(filter_stop_words)\
        .map(lambda word: (word,1))\
        .cache()          

nbMots = lines_traitement.count()

lines_fin = lines_traitement.reduceByKey(add)\
                .map(lambda (word,occ): (word,float(occ)/float(nbMots),len(word)))\
                .map(lambda word: Word(*word))

df = spark.createDataFrame(lines_fin).cache()
df.createTempView("beaugoss")
spark.sql("select * from beaugoss").show()

#df.createTempView("df_cache")
#spark.sql("select * from df_cache order by longueur desc").show()
df_1 = df.orderBy(df.freq, ascending=False)

#spark.sql("select * from df_cache where longueur=4 order by freq desc").show()
df_2 = df.filter(df.longueur == 4)\
.orderBy(df.freq, ascending=False)

#spark.sql("select * from df_cache where longueur=15 order by freq desc").show()
df_3 = df.filter(df.longueur == 15)\
.orderBy(df.freq, ascending=False)

raw_input("Appuyez sur Ctrl+C")