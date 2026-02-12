./scripts/task2_run_streaming.sh
./scripts/monthly_ride.sh
./scripts/average_trip_duration.sh
./scripts/task5_run_streaming.sh


echo "==============TASK-2 Output==============="
hdfs dfs -cat /rides/output/task2/part-00000
echo "=========================================="
echo "==============TASK-3 Output==============="
hdfs dfs -cat /rides/output/task3/part-00000 | head -20
echo "=========================================="
echo "==============TASK-4 Output==============="
hdfs dfs -cat /rides/output/task4/part-00000
echo "=========================================="
echo "==============TASK-5 Output==============="
hdfs dfs -cat /rides/output/task5/part-00000 | sort -k2 -nr | head -5
