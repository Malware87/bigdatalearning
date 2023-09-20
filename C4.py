from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col, split, explode, count, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
spark = SparkSession.builder.appName("test").getOrCreate()

# Đọc dữ liệu từ tệp CSV
dataQg = spark.read.format('csv').option('header', 'true').load(
    'data/qg_noc.csv', header=True, inferSchema=True)

dataOlympic = spark.read.format('csv').option('header', 'true').load(
    'data/vdv_olympics.csv', header=True, inferSchema=True)
# Câu 4 :
athletes_count_per_year = dataOlympic.groupBy(year(col("Year")).alias("year")) \
    .agg(count("*").alias("number_of_athletes")) .orderBy(desc("year")) .limit(5)

# Show the result
athletes_count_per_year.show()

# Đóng Spark Session
spark.stop()
