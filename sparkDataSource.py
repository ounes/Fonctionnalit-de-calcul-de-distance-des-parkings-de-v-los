# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import functions as F
import sys, re

spark = SparkSession\
    .builder.appName("TP Data Source")\
    .config("master", "local[2]")\
    .config("spark.sql.warehouse.dir", 'C:\\Users\\diginamic\\Documents\\Spark\\data')\
    .config("spark.sql.parquet.compression.codec","gzip") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.json(sys.argv[1])

'''df.select("records.fields")\
.select("fields.city","fields.date_start","fields.date_end","fields.pricing_info","fields.address","fields.department","fields.title")'''

records = df.select(explode(df.records.fields).alias("fields"))

champ = records\
.select(records.fields.city, records.fields.date_start, records.fields.date_end, records.fields.pricing_info, records.fields.address, records.fields.department, records.fields.title)

champ.printSchema()

champ.write.partitionBy("fields.department","fields.city").mode("overwrite")\
.format("parquet").saveAsTable("Events")

df2 = spark.read.parquet("C:\\Users\\diginamic\\Documents\\Spark\\data\\events\\fields.department=Bouches-du-Rhône\\fields.city=Marseille")

df3 = df2.withColumn("fields.soldout",F.lit(False))

df3.write.mode("overwrite")\
.format("parquet").saveAsTable("Events2")



#df2.filter(records.fields.city == "Marseille")

