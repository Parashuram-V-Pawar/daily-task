#!/bin/bash
INPUT_FILE=/rides/raw/uber-rides-dataset.csv
OUTPUT_FILE=/rides/output/task2

MAPPER=streaming/mapper_completed.py
REDUCER=streaming/reducer_completed.py

echo "Deleting old task2 output if exists..."
hdfs dfs -rm -r -f $OUTPUT_FILE

echo "Starting Hadoop Streaming Job..."

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-input $INPUT_FILE \
-output $OUTPUT_FILE \
-mapper "python3 mapper_completed.py" \
-combiner "python3 reducer_completed.py" \
-reducer "python3 reducer_completed.py" \
-file $MAPPER \
-file $REDUCER

echo "Showing output..."
hdfs dfs -cat $OUTPUT_FILE/part-00000
