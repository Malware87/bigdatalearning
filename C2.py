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

# Lọc ra các bản ghi của vận động viên Việt Nam tham gia Olympic trong thập kỷ 2000
vietnam_athletes = dataOlympic.filter(
    (dataOlympic["NOC"] == "VIE") & (dataOlympic["Year"] >= 2000) & (dataOlympic["Year"] < 2010))

# Hiển thị thông tin về các vận động viên Việt Nam tham gia Olympic trong thập kỷ 2000
result = vietnam_athletes.select("ID", "Name", "Sex", "Age", "Height", "Weight", "Team",
                                 "NOC", "Games", "Year", "Season", "City", "Sport", "Event", "Medal")
result.show(result.count(), truncate=False)
