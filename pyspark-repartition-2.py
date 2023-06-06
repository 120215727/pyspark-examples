# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
          .appName('SparkByExamples.com') \
          .getOrCreate()

df=spark.read.option("header",True) \
        .csv("C:/apps/sparkbyexamples/src/pyspark-examples/resources/simple-zipcodes.csv")

# dataframe的repartition方法
newDF=df.repartition(3)
print(newDF.rdd.getNumPartitions())

newDF.write.option("header",True).mode("overwrite") \
        .csv("/tmp/zipcodes-state")

# dataframe的repartition方法
df2=df.repartition(3,"state")
df2.write.option("header",True).mode("overwrite") \
   .csv("/tmp/zipcodes-state-3states")

# dataframe的repartition方法
df3=df.repartition("state")
df3.write.option("header",True).mode("overwrite") \
   .csv("/tmp/zipcodes-state-allstates")