#!/usr/bin/env python3
import os
import re
import operator
import pandas as pd

from dotenv import load_dotenv
from distutils.util import strtobool

from mapreduce import mapper, sorter, reducer


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
        for key, value in passengers:
            f.write(f"{key},{value}\n")
    return max_passenger, flight_count


def test_mapreduce():
    """Test mapreduce functions"""
    # Clear console
    cls()
    # Load environment variables from .env
    load_dotenv()
    # Initialise mapreduce file names
    mapper_data_name = os.getenv("MAPPED_DATA")
    sorted_data_name = os.getenv("SORTED_DATA")
    reduced_data_name = os.getenv("REDUCED_DATA")
    # Load user specified data directory
    data_dir = os.getenv("DATA_DIR")
    # Load passenger data with custom headers
    passenger_data = pd.read_csv(
        f"{data_dir}/AComp_Passenger_data_no_error.csv",
        names=[
            "passenger_id",
            "flight_id",
            "from_airport",
            "to_airport",
            "departure_time",
            "flight_duration",
        ],
    )

    # Map passenger data to flights
    mapper._map(passenger_data, mapper_data_name)

    # Load flights into dataframe
    flights = pd.read_csv(mapper_data_name, header=None)
    # Sort mapped data by (flight id/airport) key
    sorter._sort(flights, sorted_data_name)

    # Load sorted dataframe into dataframe
    sorted_data = pd.read_csv(sorted_data_name, header=None)
    # Reduce dataframe
    reducer._reduce(sorted_data, reduced_data_name)


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

    airports = {}
    for _, row in data.iterrows():
        airports[row["airport_code"]] = [row["airport"], row["lat"], row["long"]]
    return airports


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
    # Clear console
    cls()
    # Load environment variables from .env
    load_dotenv()
    use_hadoop_output = strtobool(os.getenv("HADOOP_DATA"))
    reduced_data_name = os.getenv("HADOOP_OUTPUT") \
        if use_hadoop_output  \
        else os.getenv("REDUCED_DATA")
    # Load user specified data directory
    data_dir = os.getenv("DATA_DIR")

    test_mapreduce()
    # Read reduced flight
    reduced_data = load_reduced_data(reduced_data_name)
    # Remove escape characters
    reduced_data = [[entry.replace("\\t", "") for entry in flight] for flight in reduced_data]
    
    # Load airports
    airport_data = pd.read_csv(
        f"{data_dir}/Top30_airports_LatLong.csv",
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
