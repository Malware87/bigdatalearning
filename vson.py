from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col, split, explode, count, lower

spark = SparkSession.builder.appName("test").getOrCreate()
dataQg = spark.read.format('csv').option('header', 'true').load(
    'qg_noc.csv', header=True, inferSchema=True)
dataOlympic = spark.read.format('csv').option('header', 'true').load(
    'vdv_olympics.csv', header=True, inferSchema=True)

# Câu 3. Sử dụng file vdv_olympics, qg_noc.csv
# 3.1. Cho biết schema của bộ dữ liệu, hiển thị số bản ghi của bộ dữ liệu.
# 3.2. Đưa ra thông tin về các vận động viên Việt Nam tham gia Olympic trong thập kỷ 2000.
# 3.3. Đưa ra thông tin về 7 quốc gia có số lượng vận đông viên đông nhất tham gia thế vận hội 2016
# 3.4. Cho biết số lượng vận động viên tham dự 5 kỳ thế vận hội gần nhất
# 3.5. Cho biết số lượng vận động viên nam tại mỗi kỳ thế vận hội trong thế kỷ 21
# +----+------------------+
# |year|number_of_athletes|
# +----+------------------+
# |2000|             XXXXX|
# |2002|              XXXX|

print("---Câu 1---")
print("Schema của bộ dữ liệu: ")
dataOlympic.printSchema()
data_count = dataOlympic.count()
print("Số bản ghi của bộ dữ liệu: ", data_count)


print("---Câu 2---")
print("Đưa ra thông tin về các vận động viên Việt Nam tham gia Olympic trong thập kỷ 2000: ")
data_2000 = dataOlympic.filter(year(col("Year")) >= 2000)
data_vn_2000 = data_2000.filter(col("Team") == "Vietnam")
data_vn_2000_next = data_vn_2000.filter(year(col("Year")) <= 2010)
data_vn_2000_next.show(data_vn_2000_next.count())
print(data_vn_2000_next.count())

# Câu 3. Sử dụng file vdv_olympics, qg_noc.csv
# 3.1. Cho biết schema của bộ dữ liệu, hiển thị số bản ghi của bộ dữ liệu.
# 3.2. Đưa ra thông tin về các vận động viên Việt Nam tham gia Olympic trong thập kỷ 2000.
# 3.3. Đưa ra thông tin về 7 quốc gia có số lượng vận đông viên đông nhất tham gia thế vận hội 2016
# 3.4. Cho biết số lượng vận động viên tham dự 5 kỳ thế vận hội gần nhất
# 3.5. Cho biết số lượng vận động viên nam tại mỗi kỳ thế vận hội trong thế kỷ 21
# +----+------------------+
# |year|number_of_athletes|
# +----+------------------+
# |2000|             XXXXX|
# |2002|              XXXX|


# 3.3
print("============= 7 quốc gia có số lượng vận đông viên đông nhất tham gia thế vận hội 2016 ==============")
# vdv_2016 = vdv.filter(year(col("date")) == 2016)
# top_7_countries = vdv_2016.groupBy("region").count().orderBy(desc("count")).limit(7)
# top_7_countries.show()
vdv_2016 = dataOlympic.filter(col("Year") == 2016)

# Group by country (NOC) and count the number of athletes
top_countries_2016 = vdv_2016.groupBy("NOC").agg(count("*").alias("number_of_athletes")).orderBy(desc("number_of_athletes")) \
                             .limit(7)

# Show the result
top_countries_2016.show()

# 3.4
print("============ Số lượng vận động viên tham dự 5 kỳ thế vận hội gần nhất ===============")
# athletes_count_per_year = vdv.groupBy(year(col("date")).alias("year")).agg(count("*").alias("number_of_athletes")).orderBy(desc("year")).limit(5)
# athletes_count_per_year.show()
# Corrected column name: Use 'Year' instead of 'date'
athletes_count_per_year = dataOlympic.groupBy(year(col("Year")).alias("year")) \
    .agg(count("*").alias("number_of_athletes")) \
    .orderBy(desc("year")) \
    .limit(5)

# Show the result
athletes_count_per_year.show()

# 3.5
print("============ Số lượng vận động viên nam tại mỗi kỳ thế vận hội trong thế kỷ 21 ===============")
athletes_count_per_year_gender = dataOlympic.filter((col("Sex") == "M") & (year(col("Year")) >= 2000)) \
    .groupBy(year(col("Year")).alias("year")) \
    .agg(count("*").alias("number_of_athletes")) \
    .orderBy("year")

# Show the result
athletes_count_per_year_gender.show()
