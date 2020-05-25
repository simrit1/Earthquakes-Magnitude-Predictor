"""
    SPARK ML
"""

# LIBRAIRIES

import findspark ; findspark.init()
import pyspark
import subprocess

from pyspark.sql            import SparkSession
from pyspark.sql.types      import *
from pyspark.sql.functions  import *

from pyspark.ml             import Pipeline
from pyspark.ml.regression  import RandomForestRegressor
from pyspark.ml.feature     import VectorAssembler
from pyspark.ml.evaluation  import RegressionEvaluator

# CONF SPARK SESSION

spark = SparkSession\
    .builder\
    .master("local[2]")\
    .appName("earthquakes_predictor")\
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:2.4.1")\
    .getOrCreate()

# LOAD TRAIN SET

print("Reading train set from Mongo DB ...")

df_train = spark.read\
    .format("mongo")\
    .option("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/Earthquakes.earthquakes")\
    .load()

df_train.show()
df_train.printSchema()

# LOAD & CLEAN TEST SET

print("Extracting & cleaning test set ...")

df_test = spark.read.csv(r'data/testset.csv', header = True)

df_test = df_test['time', 'latitude', 'longitude', 'mag', 'depth']

df_test = df_test\
    .withColumnRenamed('time',      'Date')\
    .withColumnRenamed('latitude',  'Latitude')\
    .withColumnRenamed('longitude', 'Longitude')\
    .withColumnRenamed('mag',       'Magnitude')\
    .withColumnRenamed('depth',     'Depth')

df_test = df_test\
    .withColumn('Latitude',  df_test['Latitude'].cast(DoubleType()))\
    .withColumn('Longitude', df_test['Longitude'].cast(DoubleType()))\
    .withColumn('Magnitude', df_test['Magnitude'].cast(DoubleType()))\
    .withColumn('Depth',     df_test['Depth'].cast(DoubleType()))

df_train.show()
df_train.printSchema()

# TEST / TRAIN SETS

print("Selecting features ...")

df_testing  =  df_test['Latitude', 'Longitude', 'Magnitude', 'Depth']
df_training = df_train['Latitude', 'Longitude', 'Magnitude', 'Depth']

df_testing  = df_testing.dropna()
df_training = df_training.dropna()

df_training.printSchema()
df_testing.printSchema() 

# SELECT FEATURES TO PARSE INTO MODEL

print("Running Random Forest Regressor to predict 2017 earthquakes magnitudes ...")

assembler = VectorAssembler(
    inputCols = ['Latitude', 'Longitude', 'Depth'],
    outputCol = 'Features'
)

# CREATE MODEL

modeltype = RandomForestRegressor(
    featuresCol = 'Features',
    labelCol    = 'Magnitude'
)

# CHAIN ASSEMBLER & MODEL INTO A PIPELINE

pipeline = Pipeline(stages = [assembler, modeltype])

# TRAIN AND PREDICT

model       = pipeline.fit(df_training)
predictions = model.transform(df_testing)

# MODEL EVALUATION w RMSE

evaluator = RegressionEvaluator(
    labelCol      = 'Magnitude',
    predictionCol = 'prediction',
    metricName    = 'rmse'
)
rmse = evaluator.evaluate(predictions)

# CREATE PREDICTION DATASET

df_pred = predictions['Latitude', 'Longitude', 'prediction']
df_pred = df_pred\
    .withColumnRenamed('prediction', 'Magnitude_pred')\
    .withColumn('Year', lit(2017))\
    .withColumn('RMSE', lit(rmse))


df_pred.show()
df_pred.printSchema()

print('RMSE on TEST DATA = \t', rmse)

# TO MONGO

print("Saving prediction set to MongoDB ...")

df_pred\
    .write\
    .format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Earthquakes.predictions')\
    .save()

print("All jobs done")