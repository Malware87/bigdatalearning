import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col, split, explode, count, lower, upper

# Khởi tạo Spark Session
spark = SparkSession.builder.appName("SparkExercise").getOrCreate()

# Đọc dữ liệu từ file CSV
data = spark.read.csv("ex1.csv", header=True, inferSchema=True)

# Câu 1
total_transactions = data.count()
total_products = data.select("StockCode").distinct().count()
total_customers = data.select("CustomerID").distinct().count()
print("1. Tổng số giao dịch:", total_transactions)
print("   Tổng số sản phẩm khác nhau:", total_products)
print("   Tổng số khách hàng khác nhau:", total_customers)

# Câu 2
null_customers = data.filter(col("CustomerID").isNull()).count()
total_rows = data.count()
null_customers_ratio = null_customers / total_rows
print("2. Tỉ lệ khách hàng không có thông tin:", null_customers_ratio)

# Câu 3
country_third_most_orders = data.groupBy("Country").agg(sum("Quantity").alias(
    "TotalQuantity")).orderBy(desc("TotalQuantity")).collect()[2]
print("3. Nước có số lượng đơn hàng nhiều thứ 3:",
      country_third_most_orders["Country"])

# Câu 4
words = data.select(explode(split(data["Description"], " ")).alias("word"))
word_count = words.groupBy("word").count()
least_common_word = word_count.orderBy("count").first()["word"]
print("4. Từ xuất hiện ít nhất trong Description:", least_common_word)

# Câu 5
max_quantity_product_uk = data.filter((col("Country") == "United Kingdom")).groupBy(
    "StockCode").agg(sum("Quantity").alias("TotalQuantity")).orderBy(desc("TotalQuantity")).first()
print("5. Sản phẩm bán được số lượng lớn nhất ở United Kingdom:",
      max_quantity_product_uk["StockCode"])

# Đóng Spark Session
spark.stop()
