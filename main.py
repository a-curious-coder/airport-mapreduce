#!/usr/bin/env python3

import os
import time
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from nautical_calculations.basic import get_distance
from nautical_calculations.operations import convert_to_miles

from flight import Flight


def get_flights_per_airport(airport_data, passenger_data):
    """Gets the number of flights from each airport

    Args:
        data (pd.DataFrame): Passenger flight data

    Returns:
        list, list: list of tuples containing airport counts,
        list of unused airports
    """
    start = time.perf_counter()
    # Count number of flights per airport
    counts = passenger_data["from_airport"].value_counts()
    # Extract airport names from counts index list
    airports = counts.index.tolist()
    # Zip airport names and counts together
    airports = zip(airports, counts)
    # Create list of tuples containing airport and
    # number of flights for that airport
    airport_counts = [i for i in airports]

    # Collect unique airport values from passenger data
    passenger_airports = passenger_data["from_airport"].unique()
    # Extract all airport codes that aren't in passenger airports
    unused = airport_data[~airport_data.airport_code.isin(passenger_airports)]
    # Convert unused airports to list
    unused_airports = unused["airport_code"].tolist()

    end = time.perf_counter()
    print(f"[1]\tExecuton Time: {end-start:.2f}s")

    return airport_counts, unused_airports


def get_flight_list(passenger_data):
    """Creates a list consisting of each flight with the corresponding details:
    - Flight ID
    - Passenger List
    - IATA/FAA code(s)
    - Departure time (HH:MM:SS)
    - Arrival time (HH:MM:SS)
    - Flight time (HH:MM:SS)

    Args:
        passenger_data (pd.DataFrame): Passenger / flight data

    Returns:
        list: flight data
    """
    start = time.perf_counter()
    flights = []
    # Collect each unique flight id
    flight_ids = passenger_data["flight_id"].unique()
    for flight_id in flight_ids:
        # Creat a list of passengers for this flight
        flight = passenger_data[passenger_data["flight_id"] == flight_id]
        # Create a list of passengers
        passengers = flight["passenger_id"].unique()
        # Collect airport code of starting location
        airport_code = flight["from_airport"].unique()
        #   departure time (HH:MM:SS)
        departure_times = flight["departure_time"].unique()
        # Converting seconds representation of departure time
        # to human readable time
        cdeparture_times = [
            datetime.fromtimestamp(dtime).strftime("%H:%M:%S")
            for dtime in flight["departure_time"].unique()
        ]
        #   arrival time (HH:MM:SS)
        flight_durations = [
            duration * 60 for duration in flight["flight_duration"].unique()
        ]
        # Add departure time (seconds format) to the duration of
        # the flight to get arrival times
        arrival_times = [
            departure_time + duration
            for departure_time, duration in zip(departure_times, flight_durations)
        ]
        # Converting seconds representation of
        # arrival time to human readable time
        carrival_times = [
            datetime.fromtimestamp(dtime).strftime("%H:%M:%S")
            for dtime in arrival_times
        ]
        # Convert flight duration times to human readable format
        flight_times = [
            datetime.fromtimestamp(dtime).strftime("%H:%M:%S")
            for dtime in flight_durations
        ]

        # Create list of all aforecollected attributes
        details = [
            flight,
            passengers,
            airport_code,
            cdeparture_times,
            carrival_times,
            flight_times,
        ]
        # Append flight details to flights
        flights.append(details)

    end = time.perf_counter()
    print(f"[2]\tExecution Time: {end-start:.2f}s")

    return flights


def get_highest_airmile_passenger(airport_data, passenger_data):
    """Calculates miles travelled for each passenger based on flight data

    Args:
        airport_data (pd.DataFrame): Airport data
        passenger_data (pd.DataFrame): Passenger flight data

    Returns:
        string: id of passenger with greatest distance travelled
    """
    start = time.perf_counter()
    # Get list of every flight
    flights = passenger_data[["passenger_id", "from_airport", "to_airport"]]
    # Create dictionary for each airport mapped to its coordinates
    locations = [
        (lat, long) for lat, long in zip(airport_data["lat"], airport_data["long"])
    ]
    # Map airport to lat/long location
    airport_dict = dict(zip(airport_data["airport_code"], locations))

    distances = []
    # Replace all airports with their corresponding coordinates
    for _, flight in flights.iterrows():
        # Convert from_airport values to lat/long
        flight["from_airport"] = airport_dict[flight["from_airport"]]
        # Convert to_airport values to corresponding coordinates in dictionary
        flight["to_airport"] = airport_dict[flight["to_airport"]]
        # Get nautical distance in km
        distance = get_distance(
            flight["from_airport"][0],
            flight["from_airport"][1],
            flight["to_airport"][0],
            flight["to_airport"][1],
        )
        # Convert nautical kilometres to nautical miles
        distance = convert_to_miles(distance)

        # Add this flight's distance in a list
        distances.append(distance)

    # Append distances list to dataframe as new column
    flights.insert(loc=0, column="distances", value=distances)

    # Sum of distances travelled for each passenger
    passenger_distances = {}
    for passenger in flights["passenger_id"].unique():
        # All of this passenger's flights
        passenger_flights = flights[flights["passenger_id"] == passenger]
        # Sum this passenger's distances travelled and round to 2dp
        passenger_distance = round(passenger_flights["distances"].sum(), 2)
        # Collect this passenger's total distance travelled from dictionary
        passenger_distances[passenger] = passenger_distance

    # Store max passenger distance with name in tuple
    passenger = max(passenger_distances, key=passenger_distances.get)
    # Return passenger data with most airmiles
    end = time.perf_counter()

    print(f"[3]\tExecution Time: {end-start:.2f}s")
    return passenger, passenger_distances[passenger]


def mapper(data):
    """Mapper function
    Args:
        data (pd.Dataframe): passenger data with headers
    """

    # Convert each row in dataframe to a string
    passenger_data_strings = data.to_string(
        header=False, index=False, index_names=False
    ).split("\n")

    # Separate each value in each row by a comma
    wrangled_passenger_data = [",".join(row.split()) for row in passenger_data_strings]

    # Pools
    fid_pool = []
    pid_pool = []

    for line in wrangled_passenger_data:
        # convert this line to list of the data's variables as strings
        print(f"1.\t{line}")
        elements = line.split(",")
        print(f"2.\t{elements}")
        # Put Passenger ID at the last place of the list
        elements = elements[-5:] + elements[:-5]
        print(f"3.\t{elements}")
        # Combine Flight ID & Departure Aiport Code
        results = [elements[0] + "_" + elements[1]]
        results.extend(elements[2:])
        print(f"4.\t{results}")
        line = "\t".join(results)
        current = Flight(line)
        # Mapper Output
        results = current.to_string()
        print("\t".join(results))

        # Add FLIGHT_ID & PASSENGER_ID TO THE POOL
        fid_pool.append(current.get_key())
        pid_pool.append(current.passenger_list[0])
        return

    # Output ID_POOL for cross-referencing
    print("0_flight_pool", "\t", "", sep="", end="")
    print("\t".join(list(set(fid_pool))))
    print("0_passenger_pool", "\t", "", sep="", end="")
    print("\t".join(list(set(pid_pool))))


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


def main():
    """main function"""
    # Clear console window
    cls()
    # Load in environment variables from .env
    load_dotenv()
    # Load user specified data directory
    data_dir = os.getenv("DATA_DIR")

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

    # airport_data = pd.read_csv(
    #     f"{data_dir}/Top30_airports_LatLong.csv",
    #     names=["airport", "airport_code", "lat", "long"],
    # )
    mapper(passenger_data)

    # print(airport_data.info())
    # airports = get_airports(airport_data)
    # print(*airports.items(), sep="\n")

    # # Collect data
    # flights_per_airport, unused_airports = get_flights_per_airport(
    #     airport_data, passenger_data
    # )
    # # flight_list = get_flight_list(passenger_data)
    # passenger, distance = get_highest_airmile_passenger(airport_data, passenger_data)

    # # Print data
    # print("\n-------------------------")
    # print("Flights per airport")
    # print("-------------------------")
    # print(*flights_per_airport, sep="\n")

    # print("-------------------------")
    # print("Unused airports")
    # print("-------------------------")
    # print(*unused_airports, sep="\n")

    # # print("-------------------------")
    # # print("Flight List")
    # # print("-------------------------")
    # # print(*flight_list[:1], sep="\n")

    # print("\n-------------------------")
    # print("Highest Airmile passenger")
    # print("-------------------------")
    # print(f"{passenger}\t{distance} miles")


if __name__ == "__main__":
    main()
