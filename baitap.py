import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col, split, substring, explode, count, lower, upper, regexp_replace

# Khởi tạo Spark Session
spark = SparkSession.builder.appName("SparkExercise").getOrCreate()

# Đọc dữ liệu từ file CSV
data = spark.read.csv("vdv_olympics.csv", header=True, inferSchema=True)

# Câu1:
custom_schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Sex", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Height", IntegerType(), True),
    StructField("Weight", IntegerType(), True),
    StructField("Team", StringType(), True),
    StructField("NOC", StringType(), True),
    StructField("Games", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Season", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Sport", StringType(), True),
    StructField("Event", StringType(), True),
    StructField("Medal", StringType(), True)
])
# Hiển thị schema của bộ dữ liệu
data.printSchema()
# Đếm số bản ghi trong bộ dữ liệu
record_count = data.count()
print(record_count)


# Câu 2:
# Lọc ra các bản ghi của vận động viên Việt Nam tham gia Olympic trong thập kỷ 2000
vietnam_athletes = data.filter(
    (data["NOC"] == "VIE") & (data["Year"] >= 2000) & (data["Year"] < 2010))

# Hiển thị thông tin về các vận động viên Việt Nam tham gia Olympic trong thập kỷ 2000
vietnam_athletes.select("ID", "Name", "Sex", "Age", "Height", "Weight", "Team",
                        "NOC", "Games", "Year", "Season", "City", "Sport", "Event", "Medal").show(10000)


# Câu 3:
# Lọc ra các bản ghi của Thế vận hội 2016
olympic_2016 = data.filter(data["Year"] == 2016)
# Đếm số lượng vận động viên của mỗi quốc gia tham gia Thế vận hội 2016
country_athlete_count = olympic_2016.groupBy("NOC").count()
# Sắp xếp theo số lượng vận động viên giảm dần
sorted_countries = country_athlete_count.orderBy(desc("count"))
# Lấy thông tin về 7 quốc gia có số lượng vận động viên đông nhất
top_countries = sorted_countries.limit(7)
# Hiển thị thông tin về 7 quốc gia có số lượng vận động viên đông nhất
top_countries.show()


# Câu 4:
cau4 = data.groupBy("Year").count().orderBy("Year", ascending=False).limit(5)
print(cau4.agg({"count": "sum"}).collect()[0][0])


# Dừng SparkSession
spark.stop()
