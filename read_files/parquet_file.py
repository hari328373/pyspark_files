from pyspark.sql import *
from pyspark.sql.functions import *


spark=SparkSession.builder.master("local[*]").appName("parquet").getOrCreate()
sc=spark.sparkContext

# read parquet file
df_p1 = spark.read.parquet(r"C:\\Users\HARI\Downloads\Datafiles\\userdata1.parquet")
df_p1.show(truncate=False)
print("total no.of rows",df_p1.count())


df_p2 = spark.read.parquet(r"C:\\Users\HARI\Downloads\Datafiles\\userdata2.parquet")
df_p2.show(truncate=False)
print("total no.of rows",df_p2.count())


