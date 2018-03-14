#-*- coding: utf-8 -*-
from pyspark import SparkContext
from math import radians, cos, sin, sqrt, atan2
import sys, re

sc = SparkContext(master='local[2]', appName='Meteo')
lines = sc.textFile(sys.argv[1])

def distance(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    km = 6373.0 * c
    return km

lines_map = lines.map(lambda line: line.replace("\"","").replace("[","").replace("]","").split(","))\
.map(lambda tup: (tup[1],float(re.sub('[ ]', '', tup[-2])),float(re.sub('[ ]', '', tup[-1]))))\
.map(lambda (nom,lon,lat): (distance(lon,lat,47.2063698,-1.5643358),nom))\
.filter(lambda tup: tup[0]<=10)\
.sortByKey()\
.collect()

for val in lines_map:
    print val[1].encode('utf-8','replace') + " est à " + str(int(val[0]*1000)) + " m des Machines de Nantes."