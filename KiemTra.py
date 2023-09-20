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

# Câu 6: Cho biết tổng số vận động viên tại mỗi kỳ thế vận hội trong thập kỷ 1990 của Nga.
# Tạo cột mùa dựa trên điều kiện
dataOlympic = dataOlympic.withColumn(
    "Season", when(col("Season") == "Summer", "Summer").otherwise("Winter"))

# Lọc và nhóm theo Năm và Mùa
russian_athletes_1990s = dataOlympic.filter(
    (col("Year") >= 1990) & (col("Year") < 2000) & (col("NOC") == "RUS"))

russian_athletes_1990s_grouped = russian_athletes_1990s.groupBy(
    "Year", "Season").agg(count("*").alias("TotalAthletes"))
print("Câu 6 :")
russian_athletes_1990s_grouped.show()

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
print("Câu 7 :")
winter_athletes_sorted.show(winter_athletes_sorted.count(), truncate=False)

# Câu 8: Đưa ra tên các quốc gia không có thông tin về quốc gia trong qg_noc
missing_countries = dataOlympic.join(dataQg, on=["NOC"], how="leftanti")
result = missing_countries.select("Team", "NOC")
print("Câu 8 :")
result.show(result.count(), truncate=False)

# Câu 9: Liệt kê danh sách 10 quốc gia giành nhiều huy chương vàng nhất thế vận hội mùa hè 2012.
summer_2012_gold_medals = dataOlympic.filter(
    (col("Year") == 2012) & (col("Season") == "Summer") & (col("Medal") == "Gold"))

top_10_countries_2012 = summer_2012_gold_medals.groupBy(
    "NOC").agg(count("*").alias("GoldMedals")).orderBy(col("GoldMedals").desc())
print("Câu 9 :")
top_10_countries_2012.show(10)

# Câu 10: Liệt kê 10 vận động viên có giành nhiều huy chương vàng nhất thế kỷ 20.
gold_medals_20th_century = dataOlympic.filter(
    (col("Year") >= 1900) & (col("Year") < 2000) & (col("Medal") == "Gold"))

top_10_athletes_20th_century = gold_medals_20th_century.groupBy(
    "Name").agg(count("*").alias("GoldMedals")).orderBy(col("GoldMedals").desc())
print("Câu 10 :")
top_10_athletes_20th_century.show(10)
# Đóng Spark Session
spark.stop()
