class batchWC():
	def __init__(self):
		self.dase_dir_date = '/data'

	def spark(self):
		from pysaprk.sql import SparkSession 
		return ( SparkSession.builder
							 .appName("Demo")
							 .master('local[2]')
							 .getOrCreate() )
	
	def getRawData(self,spark):
		from pyspark.sql.functions import split , explode 
		df = (
			spark.read.format("text")
					  .option("lineSep" , "true")
					  .load(f"{self.dase_dir_date}/txt/")
		)
		return (
			df.select(explode(split(df.value , " ")).alias("word"))
		)

	def getQualityData(self , raw_data):
		from pyspark.sql.functions import trim ,lower
		return (
			raw_data.select(lower(trim(raw_data.word)).alias("word"))
					.where("word is not null")
					.where(" word rlike '[a-z]'")
		)
	
	def getWordCount(self , qualitiy_data):
		return (
			qualitiy_data.groupBy("word").count()
		)

	def overwriteWordCount(self ,grouped_data):
		return (
			grouped_data.write
						.format("parquet")
						.mode("overwrite")
						.saveAsTable("emp")
		)	

	def wordCount(self):
		spark = self.spark()
		raw_data = getRawData(spark)
		quality_data = getQualityData(raw_data)
		grouped_data = getWordCount(qualitiy_data)
		overwriteWordCount(grouped_data)