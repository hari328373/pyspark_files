
from pyspark.sql import *
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[*]").appName("textfile").getOrCreate()

df_text = spark.read.text(r"C:\Users\HARI\Downloads\Datafiles\sample3.txt")
df_text.show(truncate=False)