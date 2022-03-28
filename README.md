# Map Reduce for Airport data

# Setup

Setup your virtual environment

```
pip install virtualenv

virtualenv .venv
```

Initialise virtual environment and install requirements for project

```
# Mac
source .venv/bin/activate
# Windows
.venv/Scripts/activate

pip install -r requirements.txt
```

Create environment variables file (Literally create a file called '.env') and add the following attributes(s)

```bash
WORKING_DIR = <The working directory of the project>
```

Open terminal in working directory and run project

```
python main.py
```

# Data-set Description

## AComp_Passenger_data_no_error.csv

| Column Header   |          Format |
| :-------------- | --------------: |
| passenger_id    |      XXXnnnnXXn |
| flight_id       |        XXXnnnnX |
| from_airport    |             XXX |
| to_airport      |             XXX |
| departure_time  | n (epochs time) |
| flight_duration |               n |

## Top30_airports_LatLong.csv

| Column Header | Format |
| :------------ | -----: |
| airport       |      X |
| airport_code  |    XXX |
| lat           |    n.n |
| long          |    n.n |

# Hadoop MapReduce

Using OSX, I downloaded and installed hadoop via brew

```bash
brew install hadoop
```

I executed the following command on OSX. Hadoop splits the input data into partitions and runs them through the map-reduce scripts I've made on separate threads, reducing runtime and the data.

```bash
hadoop jar <hadoop streaming jar file>
-input AComp_Passenger_data_no_error.csv
-mapper "mapreduce/mapper.py"
-combiner "mapreduce/sorter.py"
-reducer "mapreduce/reducer.py"
-output reduced
```

# Tasks

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
<!--
## Task 1

> Determine the number of flights from each airport; include a list of any airports not used.

### Results

```bash
-------------------------
Flight List
-------------------------
[('DEN', 46), ('IAH', 37), ('CAN', 37), ('ATL', 36), ('ORD', 33), ('KUL', 33), ('CGK', 27), ('JFK', 25), ('LHR', 25), ('CLT', 21), ('CDG', 21), ('PVG', 20), ('LAS', 17), ('BKK', 17), ('AMS', 15), ('FCO', 15), ('MUC', 14), ('MAD', 13), ('PEK', 13), ('HND', 13), ('MIA', 11), ('DFW', 11)]
```

## Task 2

> Create a list of flights based on the Flight id, this output should include a passenger list, relevant IATA/FAA codes, the departure time, the arrival time (times to be converted to HH:MM: SS format), and the flight times

### Results

```bash
-------------------------
Flights per airport
-------------------------
[['SQU6245R', array(['UES9151GS5', 'JBE2302VO4', 'SJD8775RZ4', 'HCA3158QA6',
       'XFG5747ZT9', 'PIT2755XC1', 'CYJ0225CH1', 'MXU9187YC7',
       'HGO4350KK1', 'PUD8209OG3', 'WBE6935NU3', 'IEG9308EA5',
       'LLZ3798PE3', 'YMH6360YP0', 'DAZ3029XA0', 'VZY2993ME1',
       'SPR4484HA6'], dtype=object), array(['DEN'], dtype=object), ['17:14:20'], ['10:43:20'], ['18:29:00']]]
```

_Only shows first item in the list_

```bash
-------------------------
Unused airports
-------------------------
['LAX', 'FRA', 'HKG', 'DXB', 'SIN', 'SFO', 'PHX', 'IST']
```

## Task 3

> Calculate the line-of-sight (nautical) miles for each flight and the total travelled by each passenger and thus output the passenger having earned the highest air miles

### Results

````bash
-------------------------
Highest Airmile passenger
-------------------------
UES9151GS5
``` -->
````
