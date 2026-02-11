#!/bin/bash

# TASK 4: Average Trip Duration Per App
# Use:
# trip_time
# Expected Output:
# Uber   18.5
# Gett   21.3
# Mapper:
# Emit (app, trip_time)
# Reducer:
# Maintain:
# total_time
# count
# Compute average = total_time / count
# Concepts Tested:
# Sum + count aggregation
# DoubleWritable
# Numeric conversions

INPUT_FILE=/rides/raw/uber-rides-dataset.csv
OUTPUT_FILE=/rides/output/task4

MAPPER_FILE=streaming/average_trip_duration_mapper.py
REDUCER_FILE=streaming/average_trip_duration_reducer.py


hdfs dfs -rm -r -f /rides/output/task4

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files $MAPPER_FILE,$REDUCER_FILE \
-mapper "python3 average_trip_duration_mapper.py" \
-reducer "python3 average_trip_duration_reducer.py" \
-input $INPUT_FILE \
-output $OUTPUT_FILE

hdfs dfs -cat $OUTPUT_FILE/part-00000