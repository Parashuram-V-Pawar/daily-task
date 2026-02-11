# Start Hadoop services.
start-dfs.sh
start-yarn.sh
# verify 
jps

hdfs dfs -ls / 

# Create directories for dataset and output files
hdfs dfs -mkdir -p /rides/raw/
hdfs dfs -mkdir -p /rides/output/
hdfs dfs -mkdir -p /rides/temp/

# Adding the file to HDFS
LOCAL_FILE = Downloads/uber-rides-dataset.csv
hdfs dfs -put $LOCAL_FILE /rides/raw/

# Configuring replication to 2.
hdfs dfs -setrep 2 /rides/raw/uber-rides-dataset.csv  

# Displaying statistical information of the dataset
hdfs fsck /rides/raw/uber-rides-dataset.csv -files -blocks -locations
