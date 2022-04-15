#!/usr/bin/env python3
""" Mapper function """
import sys
import pandas as pd
from flight import Flight
import multiprocessing
import sorter


def _map(data, ret=None, procnum=-1, file_name="mapped_data.csv", hadoop_mode=False):
    """Reformat and map flight data
    Args:
        data (pd.Dataframe): passenger data with headers
    """

    if procnum == -1:
        print("[*]\tSingle Thread Mappper")
    else:
        print("[*]\tMapper\tThread " + str(procnum))
    # Convert entire dataframe to a string, each row separated by a new line
    passenger_data = data.to_string(
        header=False, index=False, index_names=False
    ).split("\n")

    # Separate each value in each row by a comma
    passenger_data = [",".join(row.split()) for row in passenger_data]

    # Declare flight list
    flights = []

    for row in passenger_data:
        # Converts this row in the data to a list of variables as strings
        elements = row.split(",")
        # Move Passenger ID from beginning of list to last place in list
        elements = elements[-5:] + elements[:-5]
        # Merge flight id with airport code
        flight_data = [elements[0] + "_" + elements[1]]
        # Appends elements from row position 2 to flight_data
        flight_data.extend(elements[2:])
        # Convert transformed data to string as comma separated values
        row = ",".join(flight_data)
        # Initialise Flight object
        flight = Flight(row)
        # Print Mapper Output for this row
        flights.append(flight)

    if ret is None:
        # Write each flight to a mapped_data.csv file
        with open(file_name, "w", encoding="utf-8") as file:
            for flight in flights:
                file.write(str(flight) + "\n")
        flights = pd.read_csv(file_name, header=None)
        # # Sort mapped data by (flight id/airport) key
        flights = sorter._sort(flights, file_name=file_name, hadoop_mode=hadoop_mode)
    if ret is not None:
        ret[procnum] = flights


def multithread_map(partitions, map_results):
    jobs = []

    for i, partition in enumerate(partitions):
        p = multiprocessing.Process(target=_map, args=(partition,
                                                        map_results, i))
        jobs.append(p)
        p.start()

    for p in jobs:
        p.join()
    for p in jobs:
        p.terminate()


def main():
    """Main function"""
    # Read system input of file to dataframe
    data = pd.DataFrame(data=sys.stdin.read().splitlines())
    # Execute mapper
    _map(data, hadoop_mode=True)


if __name__ == "__main__":
    main()
