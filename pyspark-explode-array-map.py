# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 22:42:50 2019

@author: Naveen
"""

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})
        ]
df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df.printSchema()
df.show()

from pyspark.sql.functions import explode
df2 = df.select(df.name,explode(df.knownLanguages)) # 不包括None的
df2.printSchema()
df2.show()

from pyspark.sql.functions import explode
df3 = df.select(df.name,explode(df.properties)) # 在James Michael 之外，包括Robert这样的记录，但不包括Washington Jefferson
df3.printSchema()
df3.show()

from pyspark.sql.functions import explode_outer
""" with array """
df.select(df.name,explode_outer(df.knownLanguages)).show() # 包括None的
""" with map """
df.select(df.name,explode_outer(df.properties)).show() # 在James Michael 之外，包括Robert这样的记录，也包括Washington Jefferson


from pyspark.sql.functions import posexplode
""" with array """
df.select(df.name,posexplode(df.knownLanguages)).show()
""" with map """
df.select(df.name,posexplode(df.properties)).show()

from pyspark.sql.functions import posexplode_outer
""" with array """
df.select(df.name,posexplode_outer(df.knownLanguages)).show()

""" with map """
df.select(df.name,posexplode_outer(df.properties)).show()


"""END"""