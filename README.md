# Map Reduce for Airport data

## Setup

Setup your virtual environment

```bash
pip install virtualenv

virtualenv .venv
```

Initialise virtual environment and install requirements for project

```bash
# Mac
source .venv/bin/activate

# Windows
.venv/Scripts/activate

pip install -r requirements.txt
```

Create environment variables file (Literally create a file called '.env') and add the following attributes(s)

```yaml
DATA_DIR = data
MAPREDUCE_DIR = data/mapreduce

MAPPED_DATA = mapped_data.csv
SORTED_DATA = sorted_data.csv
REDUCED_DATA = reduced_data.csv

USE_HADOOP_OUTPUT = False
HADOOP_OUTPUT_DIR = reduced/part-00000
```

Open terminal in working directory and run project

```bash
python main.py
```

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
-combiner "mapreduce/sorter.py" \
-reducer "mapreduce/reducer.py" \
-output reduced
-->

## Tasks

<details>
<summary>Task 1</summary>

| Airport (From) | Flights |
| :------------- | ------: |
| PEK            |      70 |
| FRA            |      59 |
| LAS            |      53 |
| DFW            |      37 |
| DEN            |      36 |
| MIA            |      35 |
| DXB            |      30 |
| BKK            |      23 |
| SFO            |      23 |
| HKG            |      20 |
| FCO            |      20 |
| SIN            |      17 |
| LHR            |      16 |
| AMS            |      14 |
| MAD            |      14 |
| LAX            |      13 |
| CAN            |      13 |
| PVG            |       7 |
| ATL            |       0 |
| ORD            |       0 |
| HND            |       0 |
| CDG            |       0 |
| CGK            |       0 |
| JFK            |       0 |
| PHX            |       0 |
| IAH            |       0 |
| CLT            |       0 |
| MUC            |       0 |
| KUL            |       0 |
| IST            |       0 |

</details>

<details>
<summary>Task 2</summary>
UES9151GS5 has had the most number of flights (25)
</details>

## Data-set Description

### AComp_Passenger_data_no_error.csv

| Column Header   |          Format |
| :-------------- | --------------: |
| passenger_id    |      XXXnnnnXXn |
| flight_id       |        XXXnnnnX |
| from_airport    |             XXX |
| to_airport      |             XXX |
| departure_time  | n (epochs time) |
| flight_duration |               n |

### Top30_airports_LatLong.csv

| Column Header | Format |
| :------------ | -----: |
| airport       |      X |
| airport_code  |    XXX |
| lat           |    n.n |
| long          |    n.n |
