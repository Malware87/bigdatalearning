from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os
import time

# Khởi tạo SparkContext
sc = SparkContext("local[2]", "StreamingExample")

# Khởi tạo StreamingContext với thời gian nhỏ nhất của batch là 10 giây
ssc = StreamingContext(sc, 10)

# Đường dẫn đến thư mục "input"
folder_path = "input"

# Hàm để đếm số lượng tệp tin trong thư mục


def count_files():
    if os.path.exists(folder_path):
        file_list = os.listdir(folder_path)
        return len(file_list)
    else:
        return 0


# Tạo một DStream để đếm số lượng tệp tin
file_count_stream = ssc.textFileStream(folder_path).map(
    lambda x: ("File Count", count_files()))

# In ra số lượng tệp tin trong thư mục
file_count_stream.pprint()

# Khởi động StreamingContext
ssc.start()

try:
    while True:
        # Chạy vô hạn để duy trì Spark Streaming
        time.sleep(1)
except KeyboardInterrupt:
    # Dừng StreamingContext khi nhận được Ctrl+C
    ssc.stop()

# Dừng SparkContext
sc.stop()
