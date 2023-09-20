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
# Câu 7: Cho biết chiều cao tối thiểu, trung bình và tối đa của mỗi quốc gia tham gia thế vận hội mùa đông và sắp xếp theo thứ tự giảm dần.
# Chuyển đổi cột "Height" sang kiểu dữ liệu số, các giá trị không hợp lệ sẽ trở thành null
dataOlympic = dataOlympic.withColumn(
    "Height", dataOlympic["Height"].cast(DoubleType()))

winter_athletes = dataOlympic.filter(col("Season") == "Winter")
winter_athletes_grouped = winter_athletes.groupBy(
    "NOC").agg(
        min(col("Height")).alias("MinHeight"),
        avg(col("Height")).alias("AvgHeight"),
        max(col("Height")).alias("MaxHeight")
)

# Sử dụng .withColumnRenamed() để đổi tên cột "NOC" thành "Country"
winter_athletes_grouped = winter_athletes_grouped.withColumnRenamed(
    "NOC", "country")

winter_athletes_sorted = winter_athletes_grouped.orderBy(
    col("AvgHeight").desc())

winter_athletes_sorted.show(winter_athletes_sorted.count(), truncate=False)

# Đóng Spark Session
spark.stop()
