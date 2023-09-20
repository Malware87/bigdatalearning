from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os

# Khởi tạo SparkContext
sc = SparkContext("local[2]", "FileCount")
# Khởi tạo StreamingContext với batch interval là 1 giây
ssc = StreamingContext(sc, 1)

# Đường dẫn đến thư mục "input"
input_directory = "input"

# Tạo một DStream từ thư mục "input"
input_stream = ssc.textFileStream(input_directory)

# Hàm để in ra tên các file trong thư mục


def print_filenames(rdd):
    file_names = os.listdir(input_directory)
    print(f"Files in {input_directory}: {', '.join(file_names)}")


# Áp dụng hàm print_filenames cho mỗi RDD trong DStream
input_stream.foreachRDD(print_filenames)

# Khởi động Spark Streaming
ssc.start()

# Đợi cho quá trình tính toán kết thúc
ssc.awaitTermination()
