import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, sum
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os

# Khởi tạo SparkContext
sc = SparkContext("local[2]", "FileContent")
# Khởi tạo StreamingContext với batch interval là 1 giây
ssc = StreamingContext(sc, 1)

# Đường dẫn đến thư mục "input"
input_directory = "input"

# Tạo một DStream từ thư mục "input"
input_stream = ssc.textFileStream(input_directory)

# Khởi tạo SparkSession

spark = SparkSession.builder.appName("PremierLeagueRanking").getOrCreate()

# Hàm để xác định thứ hạng của các đội bóng


def calculate_rank(data_frame):
    ranked_data = data_frame.groupBy("HomeTeam").agg(
        sum(when(col("FTR") == "H", 3).when(col("FTR")
            == "D", 1).otherwise(0)).alias("HomePoints")
    ).orderBy(col("HomePoints").desc())

    window_spec = Window.orderBy(col("HomePoints").desc())
    ranked_data = ranked_data.withColumn("Rank", F.rank().over(window_spec))

    top_3_teams = ranked_data.filter(col("Rank") <= 3)

    return top_3_teams

# Hàm xử lý DStream


def process_stream(rdd):
    # Kiểm tra xem RDD có dữ liệu hay không
    if not rdd.isEmpty():
        # Đọc dữ liệu từ RDD thành DataFrame
        data = spark.read.csv(rdd, header=True, inferSchema=True)
        top_3_teams = calculate_rank(data)
        top_3_teams.show()


# Áp dụng hàm process_stream cho mỗi RDD trong DStream
input_stream.foreachRDD(process_stream)

# Khởi động Spark Streaming
ssc.start()

# Đợi cho quá trình tính toán kết thúc
ssc.awaitTermination()
