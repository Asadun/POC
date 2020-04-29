from pyspark.sql import SparkSession, HiveContext
spark = SparkSession.builder.master("local").\
    appName("app name").\
    config("spark.some.config.option", True).\
    enableHiveSupport().\
    getOrCreate()
df = spark.read.format("csv").options(header = True).load("s3a://sparkpoc2020/database_file")
#df.show(2)
# Save df to a new table in Hive
df.write.saveAsTable("test_dbTelecomPOC")
# Show the results using SELECT
spark.sql("select * from test_dbTelecomPOC").show()
spark.stop()

