# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

data = [('James','Smith','M',3000),
  ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200), 
]

columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
# 通过spark.createDataFrome是一个标准方式
df.show()


if 'salary1' not in df.columns:
    print("aa")
    
# Add new constanct column
from pyspark.sql.functions import lit
df.withColumn("bonus_percent", lit(0.3)) \
  .show()
# val longLength = udf((bookTitle: String, length: Int) => bookTitle.length > length)
# val booksWithLongTitle = dataFrame.filter(longLength($"title", $"10"))
# 第二句会报错，cannot resolve '10' given input columns id, title, author, price, publishedDate;
# 因为$"10" 包裹的内容，Spark会错误的以为是一个column，这时，可以使用lit(10) 表示，这是一个常量
# val booksWithLongTitle = dataFrame.filter(longLength($"title", lit(10)))
  
#Add column from existing column
df.withColumn("bonus_amount", df.salary*0.3) \
  .show()

#Add column by concatinating existing columns
from pyspark.sql.functions import concat_ws
df.withColumn("name", concat_ws(",","firstname",'lastname')) \
  .show()

#Add current date
from pyspark.sql.functions import current_date
df.withColumn("current_date", current_date()) \
  .show()

# pyspark.sql.functions 有很多function，比如current_date(),when()

from pyspark.sql.functions import when
df.withColumn("grade", \
   when((df.salary < 4000), lit("A")) \
     .when((df.salary >= 4000) & (df.salary <= 5000), lit("B")) \
     .otherwise(lit("C")) \
  ).show()
    
# Add column using select
df.select("firstname","salary", lit(0.3).alias("bonus")).show()
df.select("firstname","salary", lit(df.salary * 0.3).alias("bonus_amount")).show()
df.select("firstname","salary", current_date().alias("today_date")).show()

#Add columns using SQL
df.createOrReplaceTempView("PER") # 构建临时表
spark.sql("select firstname,salary, '0.3' as bonus from PER").show()
spark.sql("select firstname,salary, salary * 0.3 as bonus_amount from PER").show()
spark.sql("select firstname,salary, current_date() as today_date from PER").show()
spark.sql("select firstname,salary, " +
          "case salary when salary < 4000 then 'A' "+
          "else 'B' END as grade from PER").show()





