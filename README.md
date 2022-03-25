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

# Tasks

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

```bash
-------------------------
Highest Airmile passenger
-------------------------
UES9151GS5
```
