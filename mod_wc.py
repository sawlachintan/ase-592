from pyspark.sql import SparkSession
import time
import os
import pandas as pd
import random
from itertools import product

list1 = ['256m','512m', '768m', '1024m', '1280m', '1536m', '1792m', '2048m', '2304m', '2560m', '2816m', '3072m', '3328m', '3584m', '3840m', '4096m']
list1 = list1[1::2]
list3 = [1, 2, 3, 4, 5, 6, 7, 8]


combinations = list(product(list1, list3, list1, list3))

# Function to perform word count using PySpark
def word_count(spark, input_file, config_name):

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
    print(f"{config_name} Execution time: {execution_time} seconds")

    return execution_time

if __name__ == "__main__":
    # Initialize Spark session
    

    # Path to the sample data folder
    sample_data_path = "sample_data"

    # List input files in the sample data folder
    input_files = [os.path.join(sample_data_path, file) for file in os.listdir(sample_data_path)]
    random.shuffle(input_files)
    # Define configurations
    configurations = [
        {"name":{idx}, "executor_memory":val[0], 
         "executor_cores": val[1], 
         "driver_memory": val[2], 
         "num_executors": val[3]} for idx, val in enumerate(combinations)
        # {"name": "Default", "executor_memory": "1g", "executor_cores": 1, "num_executors": 2, "driver_memory": "1g"},
        # {"name": "Increased Executor Memory", "executor_memory": "2g", "executor_cores": 1, "num_executors": 2, "driver_memory": "1g"},
        # {"name": "Increased Executor Cores", "executor_memory": "1g", "executor_cores": 2, "num_executors": 2, "driver_memory": "1g"},
        # {"name": "Increased Num Executors", "executor_memory": "1g", "executor_cores": 1, "num_executors": 8, "driver_memory": "1g"},
        # {"name": "Increased Driver Memory", "executor_memory": "4g", "executor_cores": 4, "num_executors": 4, "driver_memory": "4g"}
    ]

    # Perform word count for each input file with different configurations
    results = []
    for input_file in input_files:
        print(f"\nWord count for file: {input_file}\n")
        for config in configurations:
            print(f"Configuration: {config['name']}")
            spark = SparkSession.builder.appName(f"{input_file}{config['name']}")\
                .config('spark.executor.instances', config["num_executors"])\
                .config("spark.executor.memory", config['driver_memory'])\
                .config("spark.executor.cores", config["executor_cores"])\
                .config("spark.executor.memory", config["executor_memory"])\
                    .getOrCreate()
            execution_time = word_count(spark, input_file, config['name'])
            results.append({
                "File": input_file,
                "Configuration": config['name'],
                "Execution Time (seconds)": execution_time,
                "Executor Memory": config['executor_memory'],
                "Executor Cores": config['executor_cores'],
                "Num Executors": config['num_executors'],
                "Driver Memory": config['driver_memory']
            })
            spark.stop()
            del spark

    # Display execution times in a table format
    df_results = pd.DataFrame(results)
    print("\nExecution Time Report:\n")
    print(df_results.to_string(index=False))
    df_results.to_csv('./test_results.csv',index=False)
    # Stop Spark session
    

