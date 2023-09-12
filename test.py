import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col, split, explode, count, lower, upper, regexp_replace

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
country_third_most_orders = data.groupBy("Country").agg(sum("InvoiceNo").alias(
    "TotalQuantity")).orderBy(desc("TotalQuantity")).collect()[2]
print("3. Nước có số lượng đơn hàng nhiều thứ 3:",
      country_third_most_orders["Country"])

# # Câu 4
# words = data.selectExpr(
#     "explode(split(regexp_replace(lower(Description), r'[^\\w\\s]', ''), ' ')) as word")
# word_count = words.groupBy("word").count()
# min_word_count = word_count.agg({"count": "min"}).first()[0]
# least_common_words = word_count.filter(
#     col("count") == min_word_count).collect()
# print("4. Số từ xuất hiện ít nhất là :", len(least_common_words), " từ")
# print("4. Từ xuất hiện ít nhất trong Description:")
# i = 1


# words = data.select(explode(regexp_replace(
#     lower(Description), r'[^\\w\\s]', '', " ")).alias("word"))
# word_count = words.groupBy("word").count()
# least_common_word = word_count.orderBy("count").first()["word"]
# print("4. Từ xuất hiện ít nhất trong Description:", least_common_word)


# for word in least_common_words:
#     # print(f"   Từ thứ {i}: {word['word']}, Số lần xuất hiện: {word['count']}")
#     print(f"{word['word']}, Số lần xuất hiện: {word['count']}")
#     i += 1


# Câu 5
max_quantity_product_uk = data.filter((col("Country") == "United Kingdom")).groupBy(
    "StockCode").agg(sum("Quantity").alias("TotalQuantity")).orderBy(desc("TotalQuantity")).first()
print("5. Sản phẩm bán được số lượng lớn nhất ở United Kingdom:",
      max_quantity_product_uk["StockCode"])

# Đóng Spark Session
spark.stop()
