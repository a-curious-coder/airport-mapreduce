#!/usr/bin/env python3
""" Reduce sorted mapped data """
import multiprocessing
import sys

import pandas as pd

from flight import Flight


def _reduce(flights, ret=None, procnum=-1, file_name="mapreduce_output/reduced_data.csv", hadoop_mode=False):
    """Condense flight_id

    Args:
        sorted_data (pd.DataFrame): mapped data
    """
    if procnum == -1:
        print("[*]\tSingle Thread Reducer")
    else:
        print("[*]\tReduce\tThread " + str(procnum))

    last_flight = None
    last_flight_key = None
    reduced_data = []

    # NOTE: Reduce logic relies on flight data being alphabetically sorted
    #  Otherwise the partitions aren't actually being reduced
    for flight in flights:
        # NOTE: Check if last flight checked is the same as current flight
        # If last flight is same as current flight
        if last_flight_key != flight.get_flight_key():
            if last_flight is not None:
                # Append last checked flight populated with passengers to reduced data
                reduced_data.append(last_flight)
            # Set new flight key
            last_flight = flight
            # Ensure the last flight key is set to current flight
            last_flight_key = flight.get_flight_key()
        else:
            # If last flight is current flight add passenger to last flight's passenger_list
            for passenger in flight.passenger_list:
                last_flight.add_passenger(passenger)

    # Add final flight to reduced_data
    if last_flight_key == flight.get_flight_key():
        reduced_data.append(last_flight)

    # If we're executing reduce on a single thread
    if ret is None:
        # Write reduced data to file
        with open(file_name, "w", encoding="utf-8") as file:
            for flight in reduced_data:
                file.write(str(flight)+"\n")
        # Check if file being executed via hadoop
        if hadoop_mode:
            # Write reduced data to stdout
            print(*reduced_data, sep="\n")
    else:
        # Assign reduced data 
        ret[procnum] = reduced_data


def multithread_reduce(mapped_data, ret):
    """ Multithreaded Reduce
    Args:
        mapped_data (list): list of mapped/semi-reduced data
        ret (list): list of reduce results
    """
    jobs = []

    for i, partition in enumerate(mapped_data):
        p = multiprocessing.Process(target=_reduce, args=(partition, ret, i))
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
    _reduce(data, hadoop_mode=True)


if __name__ == "__main__":
    main()
