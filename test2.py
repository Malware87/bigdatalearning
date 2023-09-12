import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("SparkExample").getOrCreate()
data = [1, 2, 3, 4, 5]
df = spark.createDataFrame(data, IntegerType())
df.show()
transformed_df = df.select(col("value") + 10)
transformed_df.show()

spark.stop()
