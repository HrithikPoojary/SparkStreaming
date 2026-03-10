from pyspark.sql import SparkSession

spark = SparkSession.builder\
		 .appName("Word_count")\
		 .master("local[*]")\
		 .enableHiveSupport()\
		 .getOrCreate()


spark.sql("select * from emp")


