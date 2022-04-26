#!/usr/bin/env python3
""" Mapper function """
import multiprocessing
import sys

import pandas as pd

import sorter
from flight import Flight


def _map(data, ret=None, procnum=None, file_name="mapreduce_output/mapped_data.csv", hadoop_mode=False):
    """Reformat and map flight data
    Args:
        data (pd.Dataframe): passenger data with headers
        ret (list): list we want to assign mapped results to during 
            multi-threaded execution
        procnum (int): thread/process number representing the index of the partition
        file_name (str): file name to save mapped data to during 
            single-threaded execution
        hadoop_mode (bool): if we are running mapper through hadoop
    """

    # If there's no processor number, it's a single threaded call
    if procnum is None:
        print("[*]\tSingle Thread Mapper")

    # If there's a valid processor number
    if type(procnum) == int:
        print("[*]\tMapper\tThread " + str(procnum))

    # Convert passenger_data's dataframe to a string
    #   each row separated by a new line
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
        flight_data = [elements[1] + "_" + elements[0]]
        # Appends elements from row position 2 to flight_data
        flight_data.extend(elements[2:])
        # Convert transformed data to string as comma separated values
        row = ",".join(flight_data)
        # Initialise Flight object
        flight = Flight(row)
        # Print Mapper Output for this row
        flights.append(flight)

    if ret is None:
        save_mapped_results(flights, file_name)
        flights = pd.read_csv(file_name, header=None)
        # # Sort mapped data by (flight id/airport) key
        flights = sorter._sort(flights, file_name=file_name, hadoop_mode=hadoop_mode)
    else:
        ret[procnum] = flights


def multithread_map(partitions, map_results):
    """Multithreaded map function
    Args:
        partitions (list): list of partitions
        map_results (list): list of lists to assign mapped results to
    """
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

    save_mapped_results(map_results, multithreaded=True)


def save_mapped_results(flight_data, file_name="mapreduce_output/mapped_data.csv", multithreaded=False):
    """Save mapped results to file
    Args:
        flight_data (list): list of Flight objects
        file_name (str): file name to save mapped data to
        multithreaded (bool): if we are running mapper through hadoop
    """
    if not multithreaded:
        # Write each flight to a mapped_data.csv file
        with open(file_name, "w", encoding="utf-8") as file:
            for flight in flight_data:
                file.write(str(flight) + "\n")
    if multithreaded:
        # NOTE: For the sake of previewing the mapped results, I will save results to a file
        # Write each flight from each list in map_results to file
        with open(file_name, "w", encoding="utf-8") as file:
            for flights in flight_data:
                for flight in flights:
                    file.write(str(flight) + "\n")


def main():
    """Main function"""
    # Read system input of file to dataframe
    data = pd.DataFrame(data=sys.stdin.read().splitlines())
    # Execute mapper
    _map(data, hadoop_mode=True)


if __name__ == "__main__":
    main()
