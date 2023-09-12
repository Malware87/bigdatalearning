import pyspark
from pyspark.sql import SparkSession

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("SparkExample").getOrCreate()

# Tạo RDD từ danh sách
rdd_from_list = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 2, 4, 6])

# In ra RDD mới không có các giá trị bằng nhau
distinct_rdd = rdd_from_list.distinct()
distinct_rdd_content = distinct_rdd.collect()
d_rdd_cl = spark.sparkContext.parallelize(distinct_rdd_content)
print("RDD mới không có các giá trị bằng nhau:", distinct_rdd_content)

# In ra RDD mới có các giá trị gấp 3 lần giá trị ban đầu
triple_rdd = rdd_from_list.map(lambda x: x * 3)
triple_rdd_content = triple_rdd.collect()
triple_rdd2 = d_rdd_cl.map(lambda x: x * 3)
triple_rdd_content2 = triple_rdd2.collect()
print("RDD mới có các giá trị gấp 3 lần giá trị ban đầu:", triple_rdd_content)
print("RDD mới có các giá trị gấp 3 lần giá trị đã lọc:", triple_rdd_content2)


# Đóng SparkSession
spark.stop()
