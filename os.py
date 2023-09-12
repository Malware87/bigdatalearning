import sys
import os
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Read Text to RDD - Python")
    sc = SparkContext(conf=conf)

    folder_path = "input"
    # read input text files present in the directory to RDD
    file_list = os.listdir("input")

    # đọc tất cả file
    for file_name in file_list:
        # lines = sc.wholeTextFiles("input")
        file_path = os.path.join(folder_path, file_name)
        file_rdd = sc.textFile(file_path)
    # collect the RDD to a list
        # llist = lines.collect()

    # print the list
        # for line in llist:
        #   print(line)
        print("Content of", file_name)
        for line in file_rdd.collect():
            print(line)
        print("\n")
