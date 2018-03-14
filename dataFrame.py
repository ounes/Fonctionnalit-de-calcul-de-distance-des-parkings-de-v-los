from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys

sc = SparkContext(master='local[2]', appName='MeteoDf')

lines = sc.textFile(sys.argv[1])

lines_map = lines.map(lambda line: (line[19:21], int(line[87:92]), int(line[92])))

spark = SparkSession\
    .builder.appName("MeteoDf")\
    .config("master", "local[2]")\
    .getOrCreate()

schema = StructType([\
StructField("month", StringType(), True),\
StructField("temperature", IntegerType(), True),\
StructField("qlty", IntegerType(), True)])

df = spark.createDataFrame(lines_map,schema)

#df.createTempView("meteo_filtre")
#meteo_cache = spark.sql("select mois,temperature from meteo_filtre where temperature <> 9999 and qualite in (0,1,4,5,9)")

#meteo_cache.createTempView("meteo")
#meteo = spark.sql("select mois,min(temperature),max(temperature),avg(temperature) from meteo group by mois order by mois")

meteo = df.filter((df.temperature <> 9999) & (df.qlty.isin([0,1,4,5,9])))\
.orderBy(df.month)\
.groupBy(df.month)\
.agg(F.max("temperature"),F.min("temperature"),F.avg("temperature"))

meteo.show()