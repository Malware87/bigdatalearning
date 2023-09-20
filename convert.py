import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col, split, explode, count, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


spark = SparkSession.builder.appName("test").getOrCreate()
dataQg = spark.read.format('csv').option(
    'header', 'true').load('data/qg_noc.csv')
dataOlympic = spark.read.format('csv').option(
    'header', 'true').load('data/vdv_olympics.csv')

dataOlympic = dataOlympic.withColumn(
    "ID", dataOlympic["ID"].cast(IntegerType()))
dataOlympic = dataOlympic.withColumn(
    "Name", dataOlympic["Name"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Sex", dataOlympic["Sex"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Age", dataOlympic["Age"].cast(IntegerType()))
dataOlympic = dataOlympic.withColumn(
    "Height", dataOlympic["Height"].cast(DoubleType()))
dataOlympic = dataOlympic.withColumn(
    "Weight", dataOlympic["Weight"].cast(DoubleType()))
dataOlympic = dataOlympic.withColumn(
    "Team", dataOlympic["Team"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "NOC", dataOlympic["NOC"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Games", dataOlympic["Games"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Year", dataOlympic["Year"].cast(IntegerType()))
dataOlympic = dataOlympic.withColumn(
    "Season", dataOlympic["Season"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "City", dataOlympic["City"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Sport", dataOlympic["Sport"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Event", dataOlympic["Event"].cast(StringType()))
dataOlympic = dataOlympic.withColumn(
    "Medal", dataOlympic["Medal"].cast(StringType()))

dataOlympic.printSchema()
dataOlympic.show()

d1 = dataOlympic.filter(col("NOC") == "United States")

dataOlympic.filter(col("NOC") == "United States").show()

d1 = dataOlympic
d1.show(d1.count(), truncate=False)
