"""
    SPARK ETL
"""

# LIBRAIRIES

import findspark ; findspark.init()
import pyspark
import subprocess

from pyspark.sql            import SparkSession
from pyspark.sql.types      import *
from pyspark.sql.functions  import *

# CONF SPARK SESSION

spark = SparkSession\
    .builder\
    .master('local[2]')\
    .appName('earthquakes_predictor')\
    .config("spark.jars.packages",      "org.mongodb.spark:mongo-spark-connector_2.12:2.4.1")\
    .getOrCreate()

# subprocess.run('pyspark --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/Earthquakes.earthquakes" --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/Earthquakes.earthquakes" --packages org.mongodb.spark:mongo-spark-connector_2.12:2.4.1', shell=True, check=True)

# LOAD DATA SET

df = spark.read.csv(r'data/database.csv', header = True)

print("Extracting data ...")

df.show()
df.printSchema()

print("Cleaning data ...")

dropcol = [
    'Depth Error', 'Time', 'Depth Seismic Stations', 'Magnitude Error',
    'Magnitude Seismic Stations', 'Azimuthal Gap', 'Root Mean Square',
    'Source', 'Location Source', 'Magnitude Source', 'Status', 
    'Horizontal Distance', 'Horizontal Error'
]

df = df.drop(*dropcol)       ; del dropcol

df = df\
    .withColumn('Latitude',  df['Latitude'].cast(DoubleType()))\
    .withColumn('Longitude', df['Longitude'].cast(DoubleType()))\
    .withColumn('Depth',     df['Depth'].cast(DoubleType()))\
    .withColumn('Magnitude', df['Magnitude'].cast(DoubleType()))

df.show()
df.printSchema()

print("Creating new dataframe freq, max, avg / year ...")

# EXTRACT YEAR FROM DATE

df = df.withColumn('Year', year(to_timestamp('Date', 'dd/MM/yyyy')))

# COUNT E.QUAKES FOR EACH YEAR

df_qfreq = df\
    .groupBy('Year')\
    .count()\
    .withColumnRenamed('count', 'Count')

# MAX & AVG FOR EACH YEAR

df_max = df\
    .groupBy('Year')\
    .max('Magnitude')\
    .withColumnRenamed('max(Magnitude)', 'Magnitude_max')

df_avg = df\
    .groupBy('Year')\
    .avg('Magnitude')\
    .withColumnRenamed('avg(Magnitude)', 'Magnitude_avg')

# JOIN

df_sum = df_qfreq\
    .join(df_max, ['Year'])\
    .join(df_avg, ['Year'])

del df_max, df_avg, df_qfreq

df_sum.show()
df_sum.printSchema()

print("Writing to Mongo DB ...")

# TO MONGO

df\
    .write\
    .format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Earthquakes.earthquakes')\
    .save()

df_sum\
    .write\
    .format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Earthquakes.summary')\
    .save()

print("All jobs done")