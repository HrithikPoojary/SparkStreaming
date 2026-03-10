from pyspark.sql import SparkSession
from pyspark.sql.functions import split ,explode , lower , trim

spark = SparkSession.builder\
		 .appName("Word_count")\
		 .master("local[*]")\
		 .getOrCreate()


dase_dir_date = '/data'

lines = (
	spark.read.format("text")
		  .option("lineSep" , ".")
		  .load(f"{dase_dir_date}/txt/")
     )

# print(lines.select(split(lines.value ," ").alias("Word")).count())

# print(lines.select(explode(split(lines.value ," ")).alias("Word")).count())


raw_words = lines.select(explode(split(lines.value ," ")).alias("word"))

quality_words = raw_words.select(lower(trim(raw_words.word)).alias("word"))\
						 .where("word is not null")\
						 .where("word rlike '[a-z]'")
						
wordCounts = quality_words.groupBy("word").count()

wordCounts.write\
		  .mode("overwrite")\
		  .saveAsTable("emp")
	



