from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from graphframes import *

if __name__ == "__main__":
	spark = SparkSession\
	.builder\
	.appName("Assignment_5")\
	.getOrCreate()

	input_path = "/Users/kalyani/Documents/Study/Big_Data/p2p-Gnutella09.txt"

	records = spark.sparkContext.textFile(input_path)

	rdd = records.map(lambda x: x.split(',')).filter(lambda x: not x[0].startswith('#')).map(lambda x: x[0].replace('\t', ' '))

	edges = rdd.map(lambda x: (x, )).toDF()
	edges = edges.withColumn('src', split(edges._1, ' ')[0]).withColumn('dst', split(edges._1, ' ')[1])
	edges = edges.drop('_1')

	vertices = edges.select(edges['src'].alias('id')) \
    .union(edges.select('dst')) \
    .distinct()

    sc.setCheckpointDir('/tmp/graphframes_cps')
    g = GraphFrame(vertices, edges)

    components = g.connectedComponents()

    grouped = components.groupby('component').count()

    descending = grouped.orderBy('count', ascending = False)

    print('Largest component is:' descending.show(1))

    ascending = grouped.orderBy('count')

    print('Smalles component is:' ascending.show(1))

    spark.stop()











