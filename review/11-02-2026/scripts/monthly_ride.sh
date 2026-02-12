#!/bin/bash

# TASK 3: Monthly Ride Volume Per App
# Business Question:
#  Which months had peak ride demand?
# Extract:
# Year-Month from trip_start_time
# Output Example:
# Uber_2016-07   12000
# Gett_2017-12    8000
# Mapper:
# Extract yyyy-mm
# Emit (app + "_" + yyyy-mm, 1)
# Reducer:
# Sum counts
# Concepts Tested:
# Date parsing
# Composite key
# Time-based analytics



HADOOP_STREAMING_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar

INPUT_PATH=/rides/raw/uber-rides-dataset.csv
INTERMEDIATE_PATH=/rides/temp/task3_intermediate
FINAL_OUTPUT_PATH=/rides/output/task3

MAPPER1=streaming/monthly_ride_mapper.py
REDUCER1=streaming/monthly_ride_reducer.py

MAPPER2=streaming/monthly_ride_second_mapper.py
REDUCER2=streaming/monthly_ride_second_reducer.py

echo "====== Cleaning old directories ======"
hdfs dfs -rm -r -f $INTERMEDIATE_PATH
hdfs dfs -rm -r -f $FINAL_OUTPUT_PATH

echo "====== Job 1: Monthly Ride Count ======"

hadoop jar $HADOOP_STREAMING_JAR \
-input $INPUT_PATH \
-output $INTERMEDIATE_PATH \
-mapper "python3 monthly_ride_mapper.py" \
-reducer "python3 monthly_ride_reducer.py" \
-file $MAPPER1 \
-file $REDUCER1

if [ $? -ne 0 ]; then
  echo "Job 1 Failed"
  exit 1
fi

echo "====== Job 2: Peak Month Per App ======"

hadoop jar $HADOOP_STREAMING_JAR \
-input $INTERMEDIATE_PATH \
-output $FINAL_OUTPUT_PATH \
-mapper "python3 monthly_ride_second_mapper.py" \
-reducer "python3 monthly_ride_second_reducer.py" \
-file $MAPPER2 \
-file $REDUCER2

if [ $? -ne 0 ]; then
  echo "Job 2 Failed"
  exit 1
fi

echo "====== Peak Output (Sample) ======"
hdfs dfs -cat ${FINAL_OUTPUT_PATH}/part-00000 | head -20

echo "====== Job Completed Successfully ======"
