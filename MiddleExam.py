from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col, split, explode, count, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Khởi tạo Session
spark = SparkSession.builder.appName("MiddleExam").getOrCreate()
# Đọc dữ liệu từ CSV
dataQg = spark.read.format('csv').option('header', 'true').option(
    'escape', '\"').load('data/qg_noc.csv', header=True, inferSchema=True)
dataOlympic = spark.read.format('csv').option('header', 'true').option(
    'escape', '\"').load('data/vdv_olympics.csv', header=True, inferSchema=True)

# Convert Data
dataOlympic = dataOlympic.withColumn(
    "ID", dataOlympic["ID"].cast(IntegerType()))
dataOlympic = dataOlympic.withColumn(
    "Name", dataOlympic["Name"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Sex", dataOlympic["Sex"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Age", dataOlympic["Age"].cast(IntegerType()))
dataOlympic = dataOlympic.withColumn(
    "Height", dataOlympic["Height"].cast(DoubleType()))
dataOlympic = dataOlympic.withColumn(
    "Weight", dataOlympic["Weight"].cast(DoubleType()))
dataOlympic = dataOlympic.withColumn(
    "Team", dataOlympic["Team"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "NOC", dataOlympic["NOC"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Games", dataOlympic["Games"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Year", dataOlympic["Year"].cast(IntegerType()))
dataOlympic = dataOlympic.withColumn(
    "Season", dataOlympic["Season"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "City", dataOlympic["City"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Sport", dataOlympic["Sport"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Event", dataOlympic["Event"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Medal", dataOlympic["Medal"].cast(StringType()))

# Câu 6:
print(" ================= Câu 6 ================= ")
# Lọc ra các dòng dữ liệu của Nga và có năm trong thập kỷ 1990
russian_athletes_1990s = dataOlympic.filter((dataOlympic["NOC"] == "RUS") & (
    dataOlympic["Year"] >= 1990) & (dataOlympic["Year"] <= 1999))
# Kết hợp dữ liệu từ hai tệp dựa trên cột "NOC"
combined_data = russian_athletes_1990s.join(dataQg, "NOC", "inner")
# Nhóm dữ liệu và tính tổng số vận động viên
total_athletes_by_season = combined_data.groupBy(
    "country", "season").count().withColumnRenamed("count", "number_of_athletes")
# Hiển thị kết quả
total_athletes_by_season.show()


# # Lọc và nhóm theo Năm và Mùa
# russian_athletes_1990s = dataOlympic.filter(
#     (col("Year") >= 1990) & (col("Year") <= 1999) & (col("NOC") == "RUS"))
# russian_athletes_1990s_grouped = russian_athletes_1990s.groupBy(
#     "Team", "Year", "Season").agg(count("*").alias("TotalAthletes"))
# russian_athletes_1990s_grouped.show()

# # Lọc và nhóm theo Năm và Mùa
# russian_athletes_1990s = dataOlympic.filter(
#     (col("Year") >= 1990) & (col("Year") < 2000) & (col("NOC") == "RUS"))
# russian_athletes_1990s_grouped = russian_athletes_1990s.groupBy(
#     "Year", "Season").agg(count("*").alias("TotalAthletes"))
# russian_athletes_1990s_grouped.show()

# Câu 7:
print(" ================= Câu 7 ================= ")
winter_athletes = dataOlympic.filter(col("Season") == "Winter")
winter_athletes_grouped = winter_athletes.groupBy(
    "Team").agg(
        min(col("Height")).alias("MinHeight"),
        avg(col("Height")).alias("AvgHeight"),
        max(col("Height")).alias("MaxHeight")
)

# Sử dụng .withColumnRenamed() để đổi tên cột "NOC" thành "Country"
winter_athletes_grouped = winter_athletes_grouped.withColumnRenamed(
    "Team", "country")

winter_athletes_sorted = winter_athletes_grouped.orderBy(
    col("AvgHeight").desc())

winter_athletes_sorted.show(winter_athletes_sorted.count(), truncate=False)


# Câu 8:
print(" ================= Câu 8 ================= ")
missing_countries = dataOlympic.join(
    dataQg, on=["NOC"], how="leftanti")
result = missing_countries.select("Team", "NOC").distinct()
result.show(result.count(), truncate=False)
print("Số kết quả:", result.count())


# Câu 9:
print(" ================= Câu 9 ================= ")
summer_2012_gold_medals = dataOlympic.filter(
    (col("Year") == 2012) & (col("Season") == "Summer") & (col("Medal") == "Gold"))
top_10_countries_2012 = summer_2012_gold_medals.groupBy(
    "NOC").agg(count("*").alias("GoldMedals")).orderBy(col("GoldMedals").desc())
top_10_countries_2012.show(10)

# Câu 10:
print(" ================= Câu 10 ================= ")
gold_medals_20th_century = dataOlympic.filter(
    (col("Year") >= 1900) & (col("Year") < 2000) & (col("Medal") == "Gold"))
top_10_athletes_20th_century = gold_medals_20th_century.groupBy(
    "Name").agg(count("*").alias("GoldMedals")).orderBy(col("GoldMedals").desc())
top_10_athletes_20th_century.show(10)

# Đóng Spark Session
spark.stop()
