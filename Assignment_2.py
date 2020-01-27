   
from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession

if __name__ == "__main__":


	spark = SparkSession\
    	.builder\
        .appName("PythonWordCount")\
        .getOrCreate()

	input_path = "/Users/kalyani/Documents/Study/Big_Data/ratings.txt"

	records = spark.sparkContext.textFile(input_path)

	tokens = records.map(lambda x: x.split())

	tokens2 = records.map(lambda x: x.split(","))
	t = tokens2.filter(lambda x: x[2]  in  ['1','2','3','4','5'])
	t2 = t.map(lambda x: (x[1], x[0]))
	t3 = t2.groupByKey()
	t4 = t3.map(lambda x: (x[0], list(x[1])))

	def unique_f(x):
		unique_set = set() 
		for z in x:
		 	unique_set.add(z)
		return len(unique_set)

	t5 = t4.map(lambda x: (x[0], len(x[1]), unique_f(x[1])))
	t6 = t5.filter(lambda x: x[2] >= 2)

	t6.saveAsTextFile("/Users/kalyani/Documents/Study/Big_Data/Output_2.txt")



