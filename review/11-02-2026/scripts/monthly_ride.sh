#!/bin/bash

HADOOP_STREAMING_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar

INPUT_PATH=/rides/raw/uber-rides-dataset.csv
INTERMEDIATE_PATH=/rides/temp
FINAL_OUTPUT_PATH=/rides/output/task3

echo "====== Cleaning old directories ======"
hdfs dfs -rm -r $INTERMEDIATE_PATH 2>/dev/null
hdfs dfs -rm -r $FINAL_OUTPUT_PATH 2>/dev/null

echo "====== Job 1: Monthly Ride Count ======"

hadoop jar $HADOOP_STREAMING_JAR \
  -input $INPUT_PATH \
  -output $INTERMEDIATE_PATH \
  -mapper monthly_ride_mapper.py \
  -reducer monthly_ride_reducer.py \
  -file monthly_ride_mapper.py\
  -file monthly_ride_reducer.py

echo "====== Job 2: Peak Month Per App ======"

hadoop jar $HADOOP_STREAMING_JAR \
  -input $INTERMEDIATE_PATH \
  -output $FINAL_OUTPUT_PATH \
  -mapper monthly_ride_second_mapper.py \
  -reducer monthly_ride_second_reducer.py \
  -file monthly_ride_second_mapper.py\
  -file monthly_ride_second_reducer.py

echo "====== Peak Output (Sample) ======"
hdfs dfs -cat ${FINAL_OUTPUT_PATH}/part-00000 | head -20

echo "====== Job Completed Successfully ======"
