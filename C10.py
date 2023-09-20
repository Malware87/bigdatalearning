from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col, split, explode, count, lower
from pyspark.sql.types import DoubleType  # Import kiểu dữ liệu số

spark = SparkSession.builder.appName("test").getOrCreate()

# Đọc dữ liệu từ tệp CSV
dataQg = spark.read.format('csv').option('header', 'true').load(
    'data/qg_noc.csv', header=True, inferSchema=True)

dataOlympic = spark.read.format('csv').option('header', 'true').load(
    'data/vdv_olympics.csv', header=True, inferSchema=True)
# Câu 10: Liệt kê 10 vận động viên có giành nhiều huy chương vàng nhất thế kỷ 20.
gold_medals_20th_century = dataOlympic.filter(
    (col("Year") >= 1900) & (col("Year") < 2000) & (col("Medal") == "Gold"))

top_10_athletes_20th_century = gold_medals_20th_century.groupBy(
    "Name").agg(count("*").alias("GoldMedals")).orderBy(col("GoldMedals").desc())

top_10_athletes_20th_century.show(10)

# Đóng Spark Session
spark.stop()
