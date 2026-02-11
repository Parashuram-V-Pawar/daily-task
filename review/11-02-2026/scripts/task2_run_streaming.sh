#!/bin/bash

echo " delete the output directory if already exits"
hdfs dfs -rm -r -f /rides/output/

echo "start generating hadoop data.."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-input /rides/raw/uber-rides-dataset.csv \
-output /rides/output \
-mapper "python3 mapper_completed.py" \
-reducer "python3 reducer_completed.py" \
-file mapper_completed.py \
-file reducer_completed.py


echo "checking if output generated"
hdfs dfs -ls /rides/output

echo "showing output"
hdfs dfs -cat /rides/output/part-00000
