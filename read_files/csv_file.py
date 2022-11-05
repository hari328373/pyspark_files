from pyspark.sql import *
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[*]").appName("csv").getOrCreate()
sc=spark.sparkContext

# read csv file
df_csv = spark.read.option("header","True").csv(r"C:\Users\HARI\Downloads\Datafiles\Iris (1).csv",inferSchema=True)
df_csv.show(truncate=False)
print("total number of rows",df_csv.count())

df_filter = df_csv.filter("PetalWidthCm > 1.0")
df_filter.show(truncate=False)

