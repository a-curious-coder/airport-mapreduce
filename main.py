#!/usr/bin/env python3
import os
import operator
import pandas as pd
import multiprocessing
from dotenv import load_dotenv
from distutils.util import strtobool
import numpy as np
import mapper
import reducer
import sorter
import flight
from operator import itemgetter

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


def multi_thread_mapreduce():
    # Load passenger_data
    passenger_data = get_passenger_data()
    dataframes = []
    # For each passenger_id
    for passenger_id in passenger_data['passenger_id'].unique():
        # create temporary dataframe for this id
        temp_data = passenger_data[passenger_data['passenger_id'] == passenger_id]
        # if flight_id in temp_data occurs more than once, delete row
        temp_data = temp_data.drop_duplicates(subset='flight_id', keep='first')
        # append temp_data to dataframes
        dataframes.append(temp_data)

    # concatenate dataframes
    passenger_data = pd.concat(dataframes)

    passenger_data.to_csv('final.csv', index=False)
    # return
    # Split passenger_data into processor count partitions
    partitions = np.array_split(passenger_data, os.cpu_count())

    # Initialise empty list of length equal to number of partitions
    map_results = multiprocessing.Manager().list([None] * len(partitions))

    # Apply mapper function to each partition
    mapper.multithread_map(partitions, map_results)

    # For each list in map results, sum lengths
    length = sum(len(result) for result in map_results)
    print(f"Map results: {length}")

    # Initialise empty list of length equal to number of partitions
    reduce_results = multiprocessing.Manager().list([None] * len(map_results))
    # Apply multithreading reduce function to each map_result
    reducer.multithread_reduce(map_results, reduce_results)

    length = sum(len(result) for result in reduce_results)
    print(f"Reduction overall size: {length}")

    # Combine each list in reduce_results
    reduced = [str(item) for sublist in reduce_results for item in sublist]
    # Convert reduced to dataframe
    reduced = pd.DataFrame(reduced)

    # # Sort to alphabetical order by airport/flight
    reduced = sorter._sort(reduced)
    # Convert sorted dataframe to list
    reduced = reduced[0].tolist()

    # Num passengers
    passengers = [f.split(",")[4:] for f in reduced]
    count = [j for sub in passengers for j in sub]
    print(f"(After sort)\tFlights/Passenger: {len(count)}")

    reduced = [flight.Flight(row) for row in reduced]

    magic = 0
    for f in reduced:
        # f.passenger_list = list(set(f.passenger_list))
        magic += len(f.passenger_list)
    print(magic)
    # Get total passengers from reduced
    passenger_count = sum(len(passenger.passenger_list) for passenger in reduced)
    print(f"Total flights: {passenger_count}")
    # Final reduce of reduced data
    reducer._reduce(reduced)


def unique_passengers(data):
    count = [item.passenger_list for item in data]
    print(len(count))
    count = list({j for sub in count for j in sub})
    print(f"Passengers: {len(count)}")


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


def load_reduced_data(file_name):
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


def main():
    """Main function"""
    global DATA_DIR
    global MULTITHREAD
    # Clear console
    cls()
    # Load environment variables from .env
    load_dotenv()
    use_hadoop_output = strtobool(os.getenv("USE_HADOOP_OUTPUT"))
    reduced_data_name = os.getenv("HADOOP_OUTPUT_DIR") \
        if use_hadoop_output  \
        else os.getenv("REDUCED_DATA")
    # Load user specified data directory
    DATA_DIR = os.getenv("DATA_DIR")

    if MULTITHREAD := strtobool(os.getenv("MULTITHREAD")):
        print("[*]\tMulti-threaded")
        multi_thread_mapreduce()
    else:
        print("[*]\tSingle-threaded")
        single_thread_mapreduce()
    # Read reduced flight
    reduced_data = load_reduced_data(reduced_data_name)
    # Remove escape characters
    reduced_data = [[entry.replace("\\t", "") for entry in flight] for flight in reduced_data]

    # Load airports
    airport_data = pd.read_csv(
        f"{DATA_DIR}/Top30_airports_LatLong.csv",
        names=["airport", "airport_code", "lat", "long"],
    )

    airports = get_airports(airport_data)

    # Task 1
    from_count = get_total_number_of_flights_from_airport(reduced_data, airports)
    print(*from_count, sep="\n")

    # Task 2
    max_passenger, flight_count = get_passenger_with_most_flights(reduced_data)
    print(f"{max_passenger} has had the most number of flights ({flight_count})")


if __name__ == "__main__":
    main()
