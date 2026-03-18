from pyspark.sql import SparkSession
spark = (
                        SparkSession.builder
                                .appName("Kafka")
                                .master("local[*]")
                                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                                .enableHiveSupport()
                                .getOrCreate()
        )

spark.sql("insert into emp values(1,'Raju')")
spark.sql("select count(*) from emp").show()

print("Good")