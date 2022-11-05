# Imports
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


#Create SparkSession
spark = SparkSession.builder \
    .appName('DataFrame')\
    .config('spark.jars', r'C:\Users\HARI\Downloads\Datafiles\spark-avro_2.11-2.4.4.jar') .getOrCreate()
df = spark.read.format('avro').load(r"C:\Users\HARI\Downloads\Datafiles\userdata1.avro")

df.show()








