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

# Lọc ra các bản ghi của Thế vận hội 2016
olympic_2016 = dataOlympic.filter(dataOlympic["Year"] == 2016)
# Đếm số lượng vận động viên của mỗi quốc gia tham gia Thế vận hội 2016
country_athlete_count = olympic_2016.groupBy("NOC").count()
# Sắp xếp theo số lượng vận động viên giảm dần
sorted_countries = country_athlete_count.orderBy(desc("count"))
# Lấy thông tin về 7 quốc gia có số lượng vận động viên đông nhất
top_countries = sorted_countries.limit(7)
# Hiển thị thông tin về 7 quốc gia có số lượng vận động viên đông nhất
top_countries.show()

# Đóng Spark Session
spark.stop()
