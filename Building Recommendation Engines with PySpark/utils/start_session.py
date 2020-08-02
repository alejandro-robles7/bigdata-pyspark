import sys
sys.path.insert(0,'../src')
#from libs import *

try:
    fh = open('../libs/playground_py36.zip', 'r')
except FileNotFoundError:
    pass

from pyspark.sql import SparkSession
from pyspark import SparkConf


try:
    spark.stop()
    print("Stopped a SparkSession")
except Exception as e:
    print("No existing SparkSession")

SPARK_DRIVER_MEMORY= "12G"
SPARK_DRIVER_CORE = "8"
SPARK_EXECUTOR_MEMORY= "9G"
SPARK_EXECUTOR_CORE = "3"


import os
os.environ["PYSPARK_PYTHON"] = "./playground_py36/bin/python"
conf = SparkConf().\
        setAppName("Building Recommendation Engines with PySpark").\
        setMaster('yarn-client').\
        set('spark.executor.cores', SPARK_EXECUTOR_CORE).\
        set('spark.executor.memory', SPARK_EXECUTOR_MEMORY).\
        set('spark.driver.cores', SPARK_DRIVER_CORE).\
        set('spark.driver.memory', SPARK_DRIVER_MEMORY).\
        set('spark.driver.maxResultSize', '0').\
        set('spark.sql.files.ignoreCorruptFiles', 'true').\
        set('spark.task.maxFailures', '10').\
        set('spark.yarn.dist.archives', '../libs/playground_py36.zip#TTD').\
        set("spark.hadoop.fs.s3a.multiobjectdelete.enable","true"). \
        set("spark.hadoop.fs.s3a.fast.upload","true"). \
        set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"). \
        set('spark.default.parallelism', '5000').\
        set('spark.sql.shuffle.partitions', '5000').\
        set("spark.sql.parquet.filterPushdown", "true"). \
        set("spark.sql.parquet.mergeSchema", "false"). \
        set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"). \
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"). \
        set("spark.speculation", "false"). \
        set('spark.jars.packages','net.snowflake:snowflake-jdbc:3.12.5,net.snowflake:spark-snowflake_2.11:2.7.1-spark_2.4')

spark = SparkSession.builder.\
    config(conf=conf).\
    getOrCreate()


sc = spark.sparkContext


def show(df, stream_name=None,stream_output_mode="append", limit=10):
    try:
        # For spark-core 
        result = df.limit(limit).toPandas()
    except Exception as e:
        # For structured-streaming
        if stream_name is None:
            stream_name = str(uuid.uuid1()).replace("-","")
        query = (
          df
            .writeStream
            .format("memory")        # memory = store in-memory table (for debugging only)
            .queryName(stream_name) # show = name of the in-memory table
            .trigger(processingTime='1 seconds') #Trigger = 1 second
            .outputMode(stream_output_mode)  # append
            .start()
        )
        while query.isActive:
            time.sleep(1)
            result = spark.sql(f"select * from {stream_name} limit {limit}").toPandas()
            print("Wait until the stream is ready...")
            if result.empty == False:
                break
        result = spark.sql(f"select * from {stream_name} limit {limit}").toPandas()
    
    return result

display = show

