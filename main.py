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
def get_total_airport_flights(reduced_data, airports):
    """Get total number of flights from airport (Task 1)

    Args:
        reduced_data (pd.DataFrame): reduced data
        airports (list): list of airports
    Return:
        dict: airports to number of flights from those airports
    """
    flight_counts = {}
    for airport in airports:
        # Initialise flight count for airport entry in dictionary
        flight_counts[airport] = 0
        for from_airport in reduced_data:
            # comma separated string to list
            from_airport = from_airport[0].split(",")
            # If airport is the same as from_airport
            if airport == from_airport[1]:
                # Increment flight count for this airport in dictionary
                flight_counts[airport] += len(from_airport) - 4

    # order flight_counts in descending order
    flight_counts = sorted(flight_counts.items(), key=lambda x: x[1], reverse=True)

    # NOTE: Keep results consistent, order by airport too
    # flight_counts to dataframe
    flight_counts = pd.DataFrame(flight_counts, columns=["airport", "flights"])
    dataframes = []
    # For each unique number of flights in flight_counts
    for i in flight_counts["flights"].unique():
        # Create temp dataframe with same flights value
        temp = flight_counts[flight_counts["flights"] == i]
        # Sort temp by airport
        temp = temp.sort_values(by="airport")
        # Append temp to dataframes
        dataframes.append(temp)
    flight_counts = pd.concat(dataframes)

    flight_counts.to_csv(f"{TASK_RESULT_DIR}/flight_count.csv", index=False)

    return flight_counts


def get_passenger_with_most_flights(data):
    """Get passenger with most flights (Task 2)

    Args:
        data (pd.DataFrame): reduced data
    Return:
        str: passenger with most flights
        int: number of flights
    """
    passengers = {}
    for f in data:
        f = f[0].split(",")
        for passenger in f[4:]:
            if passenger in passengers:
                passengers[passenger] += 1
            else:
                passengers[passenger] = 1

    # Get dictionary entry with highest value
    max_passenger = max(passengers.items(), key=operator.itemgetter(1))[0]
    flight_count = passengers[max_passenger]

    # order passengers in descending order
    passengers = sorted(passengers.items(), key=lambda x: x[1], reverse=True)

    # NOTE: Keep results consistent, order by passenger too
    # from_count to dataframe
    passengers = pd.DataFrame(passengers, columns=["passenger", "flights"])
    dataframes = []
    # For each unique number of flights in passengers
    for i in passengers["flights"].unique():
        # Create temp dataframe with same flights value
        temp = passengers[passengers["flights"] == i]
        # Sort temp by passenger
        temp = temp.sort_values(by="passenger")
        # Append temp to dataframes
        dataframes.append(temp)
    passengers = pd.concat(dataframes)

    passengers.to_csv(f"{TASK_RESULT_DIR}/passengers.csv", index=False)

    # Get max flight count from passengers
    max_flight_count = passengers["flights"].max()
    # Filter passengers with max flight count
    passengers = passengers[passengers["flights"] == max_flight_count]
    max_passenger_list = passengers["passenger"].tolist()

    return max_passenger_list, max_flight_count


def print_task_1_results(airport_flights):
    """Prints results for Task 1

    Args:
        airport_flights (pd.DataFrame): airport counts
    """
    print("\n[*]\tTask 1\n")
    print("Airport | Flights")
    print("----------------")
    airport_flights = list(zip(airport_flights["airport"],
                            airport_flights["flights"]))
    # Print each item in airport_flights
    for airport, flight_count in airport_flights:
        print(f"{airport}\t| {flight_count}")
    
    print(f"\nThe airport '{airport_flights[0][0]}' with most flights, has {airport_flights[0][1]} flights")


def print_task_2_results(passengers, flight_count):
    """Prints results for Task 2

    Args:
        passengers (list): list of passengers with most flights
        flight_count (int): number of flights
    """
    print("\n[*]\tTask 2\n")
    # If there's more than 1 passenger with most flights
    if len(passengers) > 1:
        print("Passengers")
        print("----------")
        print(*passengers, sep="\n")
        print(f"have the most number of flights ({flight_count})")
        return

    print(f"{passengers[0]} has the most number of flights ({flight_count})")


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
def single_thread_mapreduce(passenger_data):
    """Single thread mapreduce
    
    Args:
        passenger_data (pd.DataFrame): passenger data
    """
    # Load environment variables from .env
    load_dotenv()
    # Initialise mapreduce file names
    mapper_data_name = os.getenv("MAPPED_DATA")
    REDUCED_DATA_DIR = os.getenv("REDUCED_DATA_DIR")
    # Map passenger data to flights
    mapper._map(passenger_data, file_name=mapper_data_name)
    # Load sorted dataframe into dataframe
    mapped_data = pd.read_csv(mapper_data_name, header=None)
    # Reduce dataframe
    reducer._reduce(mapped_data, file_name=REDUCED_DATA_DIR)


def multi_thread_mapreduce(passenger_data):
    """Multi-thread mapreduce functions
    
    Args:
        passenger_data (pd.DataFrame): passenger data
    """
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


def init_settings():
    """Initialise settings for mapreduce"""
    # Declare global variables
    global DATA_DIR
    global TASK_RESULT_DIR
    global MULTITHREADING
    global REDUCED_DATA_DIR
    global MAPREDUCE

    # Load environment variables from .env
    load_dotenv()
    # Initialise settings from environment variables
    MAPREDUCE = strtobool(os.getenv("EXECUTE_MAPREDUCE"))
    MULTITHREADING = strtobool(os.getenv("MULTITHREAD"))
    hadoop = strtobool(os.getenv("USE_HADOOP_OUTPUT"))

    # NOTE: If we're using hadoop, assign different output directory
    # versus using python script
    REDUCED_DATA_DIR = os.getenv("HADOOP_OUTPUT_DIR") \
        if hadoop  \
        else os.getenv("REDUCED_DATA_DIR")
        
    # Load user specified data directory
    DATA_DIR = os.getenv("DATA_DIR")
    TASK_RESULT_DIR = os.getenv("TASK_RESULT_DIR")
    folders = [TASK_RESULT_DIR, "mapreduce_output"]
    for folder in folders:
        if not os.path.exists(folder):
            os.mkdir(folder)


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
    # Clear console
    cls()
    init_settings()
    passenger_data = get_passenger_data()

    # NOTE: Executes MapReduce process on a single or multiple threads
    if MAPREDUCE:
        if not MULTITHREADING:
            print("[*]\tSingle-threaded")
            single_thread_mapreduce(passenger_data)

        if MULTITHREADING:
            print("[*]\tMulti-threaded")
            multi_thread_mapreduce(passenger_data)

    # Get reduced flight data
    reduced = get_reduced_data(REDUCED_DATA_DIR)
    # Get airport data
    airport_data = get_airport_data()
    # Get airports from airport_data
    airports = get_airports(airport_data)

    # Task 1
    flight_numbers = get_total_airport_flights(reduced, airports)
    print_task_1_results(flight_numbers)

    # Task 2
    passengers, flight_count = get_passenger_with_most_flights(reduced)
    print_task_2_results(passengers, flight_count)


if __name__ == "__main__":
    main()
