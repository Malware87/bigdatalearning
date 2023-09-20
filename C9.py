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
# Câu 9: Liệt kê danh sách 10 quốc gia giành nhiều huy chương vàng nhất thế vận hội mùa hè 2012.
summer_2012_gold_medals = dataOlympic.filter(
    (col("Year") == 2012) & (col("Season") == "Summer") & (col("Medal") == "Gold"))

top_10_countries_2012 = summer_2012_gold_medals.groupBy(
    "NOC").agg(count("*").alias("GoldMedals")).orderBy(col("GoldMedals").desc())

top_10_countries_2012.show(10)

# Đóng Spark Session
spark.stop()
