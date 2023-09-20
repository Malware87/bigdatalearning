from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("PremierLeagueRanking").getOrCreate()

# Đọc dữ liệu từ tệp CSV
data = spark.read.csv("Premium_2015.csv",
                      header=True, inferSchema=True)

# Xác định thứ hạng của mỗi đội bóng dựa trên điểm số
ranked_data = data.groupBy("HomeTeam").agg(
    sum(when(col("FTR") == "H", 3).when(col("FTR")
        == "D", 1).otherwise(0)).alias("HomePoints")
).orderBy(col("HomePoints").desc())

# Sử dụng cửa sổ (window) để xếp hạng theo thứ tự
window_spec = Window.orderBy(col("HomePoints").desc())
ranked_data = ranked_data.withColumn("Rank", F.rank().over(window_spec))

# Lọc ra 3 đội bóng có thứ hạng cao nhất
top_3_teams = ranked_data.filter(col("Rank") <= 3)

# Hiển thị kết quả
top_3_teams.show()
