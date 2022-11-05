from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext


spark=SparkSession.builder.master("local[*]").appName("json").getOrCreate()
sc=spark.sparkContext

# read json file
df = spark.read.option("multiline","True").json(r"C:\\Users\HARI\Downloads\Datafiles\sample1.json")
df.show(truncate=False)
df.printSchema()


df1= spark.read.json(r"C:\\Users\HARI\Downloads\Datafiles\sample4.json",multiLine=True)
df1.show(truncate=False)
df1.printSchema()

# flattern data -->explode()
expl=df1.withColumn("pipil",explode("people")).drop("people")
expl.show(truncate=False)

#d=df1.select(explode("people").alias("pipil"))
#d.show(truncate=False)

df_final = df1.withColumn("pipil",explode("people")).withColumn("AGE",col("people.age")).\
    withColumn("FIRSTNAME",col("people.firstName")).withColumn("LASTNAME",col("people.lastName")).\
    withColumn("GENDER",col("people.gender")).withColumn("CONTACT",col("people.number")).drop("people","pipil")
df_final.show(truncate=False)
df_final.printSchema()

df_age=df_final.withColumn("result_age",explode("AGE")).withColumn('result_firstname',explode("FIRSTNAME")).\
    withColumn('result_lastname',explode("LASTNAME")).withColumn('result_gen',explode('GENDER')).\
    withColumn("res_contact",explode("CONTACT")).drop("AGE","FIRSTNAME","LASTNAME","GENDER","CONTACT")

df_age.show(truncate=False)





