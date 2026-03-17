from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .appName("Kafka")\
                    .config("spark.jars.packages" , "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
                    .master("local[*]")\
                    .getOrCreate()


BOOTSTRAP_SERVER = "localhost:9092"
JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
CLUSTER_API_KEY = "GETITFROM UI CLUSTER SETTING"
CLUSTER_API_SECRET = "GETITFROM UI CLUSER SETTING"

df =( spark.read.format("kafka")\
                .option("kafka.bootstrap.servers" , BOOTSTRAP_SERVER)\
#               .option("kafka.security.protocal" , "SASL_SSL")\
#               .option("kafka.sasl.machanism" , "PLAIN")\
#               .option("kafka.sasl.jass.config" , f"{JAAS_MODULE} required username = '{CLUSTER_API_KEY}' password = '{CLUSTER_API_SECRET  }';")
                .option("subscribe" , "invoices")
                .load()
)

print(df.columns)
# ['key', 'value', 'topic', 'partition', 'offset', 'timestamp', 'timestampType']
#   b       b         s        s           i           t              i
#   Binary(b)                                                         0 -> event created timestamp(producer) 1-> broker/consumed timestamp
