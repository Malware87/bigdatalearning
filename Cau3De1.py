from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
# 3.9
from pyspark.sql.functions import when

# 3.10- để trích xuất 1 phần của chuỗi
from pyspark.sql.functions import substring

# Tạo phiên làm việc Spark
spark = SparkSession.builder.appName("FIFA Analysis").getOrCreate()
# Lấy dữ liệu
df = spark.read.csv("Spark_FifaPlayer_2020.csv", header=True)

all_columns = df.columns
for column in all_columns:
    if df.schema[column].dataType == "double":
        df = df.withColumn(column, col(column).cast("float"))

# 3.1 Đếm số lượng cầu thủ
num_players = df.count()
print("3.1")
print("Số lượng cầu thủ:", num_players)

# 3.2 Tính số lượng câu lạc bộ
num_clubs = df.select("club").distinct().count()
print("3.2")
print("Số lượng câu lạc bộ:", num_clubs)

# 3.3 Tính độ tuổi trung bình của các cầu thủ
avg_age = df.selectExpr("avg(age)").first()[0]
max_age = df.selectExpr("max(age)").first()[0]
min_age = df.selectExpr("min(age)").first()[0]
print("3.3")
print("Độ tuổi trung bình của các cầu thủ:", round(avg_age, 2))
print("Cầu thủ có tuổi lớn nhất:", max_age)
print("Cầu thủ có tuổi nhỏ nhất:", min_age)

# 3.4 Tính chiều cao trung bình của các cầu thủ
avg_height = df.selectExpr("avg(height_cm)").first()[0]
max_height = df.selectExpr("max(height_cm)").first()[0]
min_height = df.selectExpr("min(height_cm)").first()[0]
print("3.4")
print("Chiều cao trung bình của các cầu thủ:", round(avg_height, 2), "cm")
print("Cầu thủ có chiều cao lớn nhất:", max_height, "cm")
print("Cầu thủ có chiều cao nhỏ nhất:", min_height, "cm")

# 3.5Tính số lượng cầu thủ tương ứng với từng quốc gia
so_cau_thu = df.groupBy("nationality").count()

# Hiển thị kết quả
print("3.5")
so_cau_thu.show(so_cau_thu.count())


# Lọc ra cầu thủ người Brazil
brazilian_players = df.filter(df["nationality"] == "Brazil")

# Tìm giá trị `value_eur` lớn nhất trong số cầu thủ Brazil
max_value_eur = brazilian_players.select(
    max(col("value_eur").cast("float"))).collect()[0][0]

# Lọc ra các cầu thủ Brazil có `value_eur` bằng giá trị lớn nhất
highest_valued_brazilian_players = brazilian_players.filter(
    col("value_eur").cast("float") == max_value_eur)

# Chọn chỉ các cột cần hiển thị
selected_columns = ["sofifa_id", "player_url", "short_name",
                    "long_name", "age", "dob", "height_cm", "weight_kg", "value_eur"]
result_df = highest_valued_brazilian_players.select(*selected_columns)

# In ra thông tin các cầu thủ Brazil có `value_eur` cao nhất
print("3.6: Cầu Thủ Có Giá Trị Cao Nhất")
result_df.show()


# 3.7 Nhóm cầu thủ theo quốc tịch và câu lạc bộ, sau đó đếm số lượng cầu thủ Brazil trong từng câu lạc bộ
brazilian_players_in_clubs = df.filter(
    df["nationality"] == "Brazil").groupBy("club").count()

# In ra số lượng cầu thủ Brazil trong các câu lạc bộ
print("3.7 Số lượng cầu thủ Brazil trong các câu lạc bộ:")
brazilian_players_in_clubs.show(brazilian_players_in_clubs.count())
# Lọc ra câu lạc bộ có số lượng cầu thủ Brazil lớn nhất
max_brazilian_players_count = brazilian_players_in_clubs.select(
    max(col("count"))).collect()[0][0]
club_with_most_brazilian_players = brazilian_players_in_clubs.filter(
    col("count") == max_brazilian_players_count)

# In ra câu lạc bộ có số lượng cầu thủ Brazil lớn nhất
print(" Câu lạc bộ có số lượng cầu thủ Brazil lớn nhất:")
club_with_most_brazilian_players.show()


# 3.8
# Sắp xếp DataFrame theo số lượng bàn thắng giảm dần và lấy top 5
top_scorers = df.orderBy(col("shooting").desc()).limit(5)

# Chọn chỉ các cột cần hiển thị
selected_columns = ["sofifa_id", "player_url", "short_name",
                    "long_name", "age", "dob", "height_cm", "weight_kg", "shooting"]

# Hiển thị danh sách top 5 cầu thủ ghi nhiều bàn thắng nhất
print('3.8: danh sách top 5 cầu thủ ghi nhiều bàn thắng nhất:')
top_scorers.select(*selected_columns).show()

# # Tìm giá trị max của trường 'shooting'
# max_shooting = df.agg(max("shooting")).collect()[0][0]

# # Lọc ra cầu thủ có số lượng bàn thắng bằng giá trị max của 'shooting'
# top_scorers = df.filter(df["shooting"] == max_shooting)

# # Chọn chỉ các cột cần hiển thị
# selected_columns = ["sofifa_id", "player_url", "short_name", "long_name", "age", "dob", "height_cm", "weight_kg","shooting"]

# # Hiển thị danh sách cầu thủ có số lượng bàn thắng cao nhất
# top_scorers.select(*selected_columns).show()


# 3.9
# Tạo cột mới 'age_group' dựa trên độ tuổi của cầu thủ
df = df.withColumn("age_group", when(df["age"] < 25, "Y").when(
    (df["age"] >= 25) & (df["age"] <= 30), "T").otherwise("O"))

# In ra cột mới 'age_group'
print("3.9: Cột mới 'age_group':")
# Lấy 20 dòng đầu tiên và chỉ hiển thị các trường thông tin được yêu cầu
top_20_players = df.select("sofifa_id", "short_name",
                           "long_name", "age", "age_group").limit(20)

# In ra thông tin của 20 cầu thủ
top_20_players.show()


# 3.10
# Tạo cột mới 'nationality_prefix' chứa 2 ký tự đầu của trường 'nationality'
df = df.withColumn("nationality_prefix", substring(df["nationality"], 1, 2))

# In ra cột mới 'nationality_prefix'
# Chọn các trường thông tin bạn muốn hiển thị
selected_columns = ["sofifa_id", "player_url", "short_name", "long_name",
                    "age", "dob", "height_cm", "weight_kg", "nationality", "nationality_prefix"]

# Hiển thị thông tin của các cầu thủ chỉ với các trường bạn đã chọn
print("3.10: 2 ký tự đầu của trường 'nationality' ")
df.select(*selected_columns).show()


# 3.10
