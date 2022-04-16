# Airport Map Reduce

This project contains a single and multi-threaded Map-Reduce emulation on a airport flights data-set via Python.

## Data-set descriptions

The data used in this project is split into two files; passenger and airport data.

### Passenger Data

| Column Header   |          Format |
| :-------------- | --------------: |
| passenger_id    |      XXXnnnnXXn |
| flight_id       |        XXXnnnnX |
| from_airport    |             XXX |
| to_airport      |             XXX |
| departure_time  | n (epochs time) |
| flight_duration |               n |

### Airport Data

| Column Header | Format |
| :------------ | -----: |
| airport       |      X |
| airport_code  |    XXX |
| lat           |    n.n |
| long          |    n.n |

## Setup

First we set up the virtual environment to download and install our Python libraries to.

```bash
# Install the virtualenv library
pip install virtualenv
# Create a new virtual environment, '.venv'
virtualenv .venv
```

Activate '.venv' and install the required libraries for the project

```bash
# Mac
source .venv/bin/activate

# Windows
.venv/Scripts/activate

# Install the required libraries via the listed libraries in 'requirements.txt'
pip install -r requirements.txt
```

Run the project

```bash
python main.py
```

## Settings

Adjustment of settings should be done within the '.env' file.
| Setting | Description | Datatype |
| :------------- | ------ | ------: |
|DATA_DIR | Directory the data-set is stored | string|
|MAPREDUCE_DIR | Stores the output of each stage in the map-reduce process | string|
|TASK_RESULT_DIR | Directory task results are saved to | string
|MAPPED_DATA_DIR | file-name for MAPPED output | string|
|SORTED_DATA_DIR | file-name for SORTED output | string|
|REDUCED_DATA_DIR | file-name for REDUCED output | string|
|USE_HADOOP_OUTPUT | states whether program should use hadoop's mapreduce output | boolean|
|HADOOP_OUTPUT_DIR| output directory of hadoop's map-reduce process| string|
|EXECUTE_MAPREDUCE| States whether to execute mapreduce process| boolean|
| MULTITHREAD| States whether multithreading will be used during map-reduce process' run-time| boolean|

## Hadoop MapReduce

Using OSX, I downloaded and installed hadoop via brew

```bash
brew install hadoop
```

The following command takes the data and places it in the Hadoop Distributed File System (HDFS). This means it becomes accessible in the working directory you're in.

```bash
hadoop fs -put data/AComp_Passenger_data_no_error.csv
```

Remove pre-existing instances of the hadoop mapreduce output

```bash
rm -rf reduced
```

I executed the following command on OSX. Hadoop splits the input data into partitions and runs them through the map-reduce scripts I've made on separate threads, reducing runtime and the data.

```bash
hadoop jar <hadoop streaming jar file directory> \
-input AComp_Passenger_data_no_error.csv \
-mapper "mapper.py" \
-combiner "sorter.py" \
-reducer "reducer.py" \
-output reduced
```

<!-- MAC
hadoop jar /usr/local/Cellar/hadoop/3.3.2/libexec/share/hadoop/tools/lib/hadoop-streaming-3.3.2.jar \
-input AComp_Passenger_data_no_error.csv \
-mapper "mapreduce/mapper.py" \
-combiner "mapreduce/combiner.py" \
-reducer "mapreduce/reducer.py" \
-output reduced
-->

## Tasks

### Task 1

| Airport (From) | Flights |
| :------------- | ------: |
| PEK |       54|
| FRA |       47|
| LAS |       37|
| MIA |       31|
| DEN |       30|
| DFW |       25|
| DXB |       23|
| SFO |       22|
| FCO |       17|
| BKK |       15|
| LHR |       15|
| HKG |       14|
| CAN |       11|
| LAX |       11|
| SIN |       11|
| AMS |       10|
| MAD |       10|
| PVG |        6|
| ATL |        0|
| CDG |        0|
| CGK |        0|
| CLT |        0|
| HND |        0|
| IAH |        0|
| IST |        0|
| JFK |        0|
| KUL |        0|
| MUC |        0|
| ORD |        0|
| PHX |        0|

The airport 'PEK' with most flights, has 54 flights

---

### Task 2

Passengers

- DAZ3029XA0
- EZC9678QI6
- HCA3158QA6
- SPR4484HA6
- UES9151GS5

have the most number of flights (17)