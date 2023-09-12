from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # Tạo Spark context với cấu hình
    conf = SparkConf().setAppName("Read All Text Files to Single RDD - Python")
    sc = SparkContext(conf=conf)

    # Đường dẫn đến thư mục chứa tất cả các tệp văn bản
    directory_path = "data/rdd/TXT"

    # Sử dụng wholeTextFiles để đọc tất cả các tệp văn bản trong thư mục
    # Kết quả là một RDD với các cặp (tên tệp, nội dung tệp)
    file_rdd = sc.wholeTextFiles(directory_path)

    # Thu thập RDD thành một danh sách
    file_list = file_rdd.collect()

    # In danh sách các cặp (tên tệp, nội dung tệp)
    for file_name, file_content in file_list:
        print(f"File: {file_name}")
        print(f"Content:\n{file_content}\n")

    sc.stop()
