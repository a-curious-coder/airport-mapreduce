#!/usr/bin/env python3
import multiprocessing
import operator
import os
from distutils.util import strtobool

import numpy as np
import pandas as pd
from dotenv import load_dotenv

import combiner
import flight
import mapper
import reducer
import sorter


# Tasks
def get_total_number_of_flights_from_airport(reduced_data, airports):
    """Get total number of flights from airport (Task 1)

    Args:
        reduced_data (pd.DataFrame): reduced data
        airports (list): list of airports
    Return:
        dict: airports to number of flights from those airports
    """
    from_count = {}
    for airport in airports:
        # Initialise from count for airport entry in dictionary
        from_count[airport] = 0
        for from_airport in reduced_data:
            # comma separated string to list
            from_airport = from_airport[0].split(",")
            if airport == from_airport[1]:
                from_count[airport] += len(from_airport) - 4

    # order from_count in descending order
    from_count = sorted(from_count.items(), key=lambda x: x[1], reverse=True)

    # Save results to file
    with open("from_count.csv", "w", encoding="utf-8") as f:
        for key, value in from_count:
            f.write(f"{key},{value}\n")

    return from_count


def get_passenger_with_most_flights(data):
    """Get passenger with most flights (Task 2)

    Args:
        data (pd.DataFrame): reduced data
    Return:
        str: passenger with most flights
        int: number of flights
    """
    passengers = {}
    for flight in data:
        flight = flight[0].split(",")
        for passenger in flight[4:]:
            if passenger in passengers:
                passengers[passenger] += 1
            else:
                passengers[passenger] = 1

    # Get dictionary entry with highest value
    max_passenger = max(passengers.items(), key=operator.itemgetter(1))[0]
    flight_count = passengers[max_passenger]

    # order passengers in descending order
    passengers = sorted(passengers.items(), key=lambda x: x[1], reverse=True)

    # Save results to file
    with open("passengers.csv", "w", encoding="utf-8") as f:
        f.write("passenger,flights\n")
        for key, value in passengers:
            f.write(f"{key},{value}\n")
    return max_passenger, flight_count


# Get/Load data
def get_passenger_data():
    """ Load passenger data from csv file
    Returns:
        pd.DataFrame: passenger data
    """
    return pd.read_csv(
        f"{DATA_DIR}/AComp_Passenger_data_no_error.csv",
        names=[
            "passenger_id",
            "flight_id",
            "from_airport",
            "to_airport",
            "departure_time",
            "flight_duration",
        ],
    )


def get_airport_data():
    """ Loads airport data from csv file
    Returns:
        pd.DataFrame: airport data
    """
    # Load airports
    return pd.read_csv(
        f"{DATA_DIR}/Top30_airports_LatLong.csv",
        names=["airport", "airport_code", "lat", "long"],
    )


def get_reduced_data(file_name):
    """Load in reduced data from csv file
    Returns:
        pd.DataFrame: reduced data
    """
    reduced_data = []
    with open(file_name, "r", encoding="utf-8") as file:
        data = file.readlines()

    for line in data:
        elements = line.strip().split("\t", 4)
        reduced_data.append(elements)

    return reduced_data


# MapReduce functions
def single_thread_mapreduce():
    """Test mapreduce functions"""
    # Load environment variables from .env
    load_dotenv()
    # Initialise mapreduce file names
    mapper_data_name = os.getenv("MAPPED_DATA")
    reduced_data_name = os.getenv("REDUCED_DATA")
    # Load passenger data with custom headers
    passenger_data = get_passenger_data()
    print(len(passenger_data['passenger_id'].unique()))
    # Map passenger data to flights
    mapper._map(passenger_data, file_name=mapper_data_name)
    # Load sorted dataframe into dataframe
    mapped_data = pd.read_csv(mapper_data_name, header=None)
    # Reduce dataframe
    reducer._reduce(mapped_data, file_name=reduced_data_name)


def multi_thread_mapreduce():
    # Load passenger_data
    passenger_data = get_passenger_data()

    # Split passenger_data into processor count partitions
    partitions = np.array_split(passenger_data, os.cpu_count())

    # Initialise empty list of length equal to number of partitions
    map_results = multiprocessing.Manager().list([None] * len(partitions))

    # Apply mapper function to each partition
    mapper.multithread_map(partitions, map_results)

    # NOTE: Reduce all data partitions
    # Initialise empty list of length equal to number of partitions
    reduce_results = multiprocessing.Manager().list([None] * len(map_results))
    # Apply multithreading reduce function to each map_result
    reducer.multithread_reduce(map_results, reduce_results)

    # Partition count is same as cpu core count
    partitions = os.cpu_count()
    while partitions > 1:
        # Right bit shift to halve the value each loop until 0
        partitions = partitions >> 1
        # Stores processed reduce results as backup
        temp = reduce_results
        # Initialise empty list of length equal to number of new partitions
        reduce_results = multiprocessing.Manager().list([None] * partitions)
        # For each pair of lists in temp
        combiner.multithread_combine(temp, reduce_results)

        if partitions != 1:
            reducer.multithread_reduce(reduce_results, reduce_results)

    # NOTE: Final reduce on last data partition
    # Combine each list in reduce_results
    reduced = [str(item) for sublist in reduce_results for item in sublist]
    # Convert reduced to dataframe
    reduced = pd.DataFrame(reduced)
    # Sort reduced data in alphabetical order for airport/flight
    reduced = sorter._sort(reduced)
    # Convert sorted  to list
    reduced = reduced[0].tolist()
    # Convert list of flight strings to flight objects
    reduced = [flight.Flight(row) for row in reduced]
    # Final reduce of combined reduced partitions
    reducer._reduce(reduced)


# Misc functions
def cls():
    """Clears console - useful for debugging/testing"""
    os.system("cls" if os.name == "nt" else "clear")


def get_airports(data):
    """Data Wrangling to match airport_code to corresponding airport/lat/long

    Args:
        data (pd.DataFrame): Airport data

    Returns:
        dict: airport and corresponding airports' data
    """

    return {row["airport_code"]: [row["airport"], row["lat"], row["long"]] for _, row in data.iterrows()}


def main():
    """Main function"""
    global DATA_DIR
    # Clear console
    cls()
    # Load environment variables from .env
    load_dotenv()
    # Initialise settings from environment variables
    multithreading = strtobool(os.getenv("MULTITHREAD"))
    use_hadoop_output = strtobool(os.getenv("USE_HADOOP_OUTPUT"))
    reduced_data_name = os.getenv("HADOOP_OUTPUT_DIR") \
        if use_hadoop_output  \
        else os.getenv("REDUCED_DATA")
    # Load user specified data directory
    DATA_DIR = os.getenv("DATA_DIR")

    if multithreading:
        print("[*]\tMulti-threaded")
        multi_thread_mapreduce()
    else:
        print("[*]\tSingle-threaded")
        single_thread_mapreduce()

    # Get reduced flight
    reduced = get_reduced_data(reduced_data_name)
    # Get airport data
    airport_data = get_airport_data()
    # Get airports from airport_data
    airports = get_airports(airport_data)

    # Task 1
    from_count = get_total_number_of_flights_from_airport(reduced, airports)
    print(*from_count, sep="\n")

    # Task 2
    max_passenger, flight_count = get_passenger_with_most_flights(reduced)
    print(f"{max_passenger} has the most number of flights ({flight_count})")


if __name__ == "__main__":
    main()
