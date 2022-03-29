# Put data file in HDFS
hadoop fs -put data/AComp_Passenger_data_no_error.csv

# Remove instances of mapreduce output folders
rm -rf reduced
echo "Using Hadoop Streaming file at: $1"

# Execute MapReduce job
hadoop jar $1 \
-input AComp_Passenger_data_no_error.csv \
-mapper "mapper.py" \
-reducer "reducer.py" \
-output reduced

# Cleanup 
rm AComp_Passenger_data_no_error.csv
rm .AComp_Passenger_data_no_error.csv.crc