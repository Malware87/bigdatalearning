import pyspark
from pyspark.sql import SparkSession

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("SparkExample").getOrCreate()

# Tạo RDD từ danh sách
rdd_from_list = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6])

# In nội dung của RDD
rdd_content = rdd_from_list.collect()
print("Nội dung của RDD:", rdd_content)

# Lọc các số chẵn
even_rdd = rdd_from_list.filter(lambda x: x % 2 == 0)

# Hiển thị các số chẵn sử dụng foreach
even_rdd.foreach(lambda x: print(x))

# Đóng SparkSession
spark.stop()
