import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os

# Khởi tạo SparkContext
sc = SparkContext("local[2]", "FileContent")
# Khởi tạo StreamingContext với batch interval là 20 giây
ssc = StreamingContext(sc, 20)

# Đường dẫn đến thư mục "input"
input_directory = "input"

# Tạo một DStream từ thư mục "input"
input_stream = ssc.textFileStream(input_directory)

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("PremierLeagueRanking").getOrCreate()

# Hàm để xác định thứ hạng của các đội bóng


def calculate_rank(data_frame):
    ranked_data = data_frame.groupBy("HomeTeam").agg(
        F.sum(F.when(data_frame["FTR"] == "H", 3)
              .when(data_frame["FTR"] == "D", 1)
              .otherwise(0)).alias("HomePoints")
    ).orderBy(F.desc("HomePoints"))
    top_3_teams = ranked_data.limit(3)  # Giới hạn kết quả cho 3 đội hàng đầu
    return top_3_teams

# Hàm để in ra nội dung của các file trong thư mục


def print_file_contents(rdd):
    file_names = os.listdir(input_directory)
    for file_name in file_names:
        file_path = os.path.join(input_directory, file_name)
        data = spark.read.csv(file_path, header=True, inferSchema=True)
        print("File:", file_name)
        top3 = calculate_rank(data)
        top3.show()


# Áp dụng hàm print_file_contents cho mỗi RDD trong DStream
input_stream.foreachRDD(print_file_contents)

# Khởi động Spark Streaming
ssc.start()

# Đợi cho quá trình tính toán kết thúc
ssc.awaitTermination()
