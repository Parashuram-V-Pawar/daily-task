#!/bin/bash

# TASK 5: Top 5 Most Active Drivers
# Business Question:
#  Which drivers completed the highest number of trips?
# Step 1:
#  Mapper:
# Emit (driver_uid, 1)
# Reducer:
# Emit (driver_uid, total_trips)
# Step 2:
#  Sort results in descending order and output Top 5.
# Options:
# Custom Comparator
# Use TreeMap in Reducer
# Chain second MapReduce job

INPUT_FILE=/rides/raw/uber-rides-dataset.csv
OUTPUT_FILE=/rides/output/task5

MAPPER=streaming/top5_mapper.py
REDUCER=streaming/top5_reducer.py

echo "delete the output directory if already exists"
hdfs dfs -rm -r -f $OUTPUT_FILE

echo "start generating hadoop data.."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-input $INPUT_FILE \
-output $OUTPUT_FILE \
-mapper "python3 top5_mapper.py" \
-reducer "python3 top5_reducer.py" \
-file $MAPPER \
-file $REDUCER

echo "showing output"
hdfs dfs -cat /rides/output/task5/part-00000 | sort -k2 -nr | head -5

