from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col, split, explode, count, lower

spark = SparkSession.builder.appName("test").getOrCreate()

# Đọc dữ liệu từ tệp CSV
dataQg = spark.read.format('csv').option('header', 'true').load(
    'data/qg_noc.csv', header=True, inferSchema=True)

dataOlympic = spark.read.format('csv').option('header', 'true').load(
    'data/vdv_olympics.csv', header=True, inferSchema=True)

# Tạo cột mùa dựa trên điều kiện
dataOlympic = dataOlympic.withColumn(
    "Season", when(col("Season") == "Summer", "Summer").otherwise("Winter"))

# Lọc và nhóm theo Năm và Mùa
russian_athletes_1990s = dataOlympic.filter(
    (col("Year") >= 1990) & (col("Year") < 2000) & (col("NOC") == "RUS"))

russian_athletes_1990s_grouped = russian_athletes_1990s.groupBy(
    "Year", "Season").agg(count("*").alias("TotalAthletes"))

russian_athletes_1990s_grouped.show()
# Đóng Spark Session
spark.stop()
