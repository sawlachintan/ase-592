from pyspark.sql import SparkSession
import time
import os
import sys
import random

def word_count(spark, input_file):

    # Set Spark configurations
    # spark.conf.set("spark.executor.memory", executor_memory)
    # spark.conf.set("spark.executor.cores", executor_cores)
    # spark.conf.set("spark.executor.instances", num_executors)
    # spark.conf.set("spark.driver.memory", driver_memory)
    
    start_time = time.time()
    # Read input text file
    lines = spark.read.text(input_file).rdd.map(lambda r: r[0])
    
    # Perform word count
    word_counts = lines.flatMap(lambda line: line.split(" ")) \
                       .map(lambda word: (word, 1)) \
                       .reduceByKey(lambda a, b: a + b)

    # Calculate and display execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")

    return execution_time

if __name__ == "__main__":
    args = sys.argv
    print(args[-1])
    spark = SparkSession.builder.appName(f"ndf")\
                .config("spark.executor.memory", args[1])\
                .config('spark.executor.instances', int(args[2]))\
                .config("spark.executor.memory", args[3])\
                .config("spark.executor.cores", int(args[4]))\
                .getOrCreate()
    word_count(spark, args[-1])