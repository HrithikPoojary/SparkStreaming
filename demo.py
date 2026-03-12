def spark():
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    # Stop existing session if present
    try:
        SparkSession.getActiveSession().stop()
    except:
        pass

    builder = (
        SparkSession.builder
        .appName("Demo")
        .master("local[2]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.range(5).write.format("delta").mode("overwrite").save("file:///home/hrithik_poojary/spark_meta/spark_warehouse/test/delta_test")
    spark.read.format("delta").load("file:///home/hrithik_poojary/spark_meta/spark_warehouse/test/delta_test").show()

    return spark

spark()

