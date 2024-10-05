from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

df = spark.read \
    .option('header', 'true') \
    .csv('hdfs://namenode:9000/data/mgcrp/breweries.csv')

df.groupby('state') \
    .count() \
    .repartition(1) \
    .write \
    .mode('overwrite') \
    .option('header', 'true') \
    .csv('hdfs://namenode:9000/data/mgcrp/breweries_groupby_pySpark.csv')