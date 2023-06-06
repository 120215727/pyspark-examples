# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com') \
        .master("local[5]").getOrCreate()

df=spark.range(0,20)
print(df.rdd.getNumPartitions())

df.write.mode("overwrite").csv("/Users/dengshewei/test/spark/")
# 会产生8个文件 part-00000-5cf3c3b2-3a86-4a89-8c90-bfd28b627cc4-c000.csv
# 和一个_SUCCESS文件

df2 = df.repartition(6)
print(df2.rdd.getNumPartitions())

df3 = df.coalesce(2)
print(df3.rdd.getNumPartitions())

df4 = df.groupBy("id").count()
print(df4.rdd.getNumPartitions())