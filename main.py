#!/usr/bin/env python3

import operator
import os

import pandas as pd
from dotenv import load_dotenv
from pandas.api.types import CategoricalDtype

from flight import Flight


def mapper(data):
    """Reformat and map flight data
    Args:
        data (pd.Dataframe): passenger data with headers
    """

    print("[*]\tMapper")
    # Convert entire dataframe to a string, each row separated by a new line
    passenger_data_strings = data.to_string(
        header=False, index=False, index_names=False
    ).split("\n")

    # Separate each value in each row by a comma
    passenger_data = [",".join(row.split()) for row in passenger_data_strings]

    # Declare flight list
    flights = []

    for row in passenger_data:
        # Convert this row in the passenger_data to list of variables as strings
        elements = row.split(",")
        # Move Passenger ID from the beginning of the list to the last place in the list
        elements = elements[-5:] + elements[:-5]
        # Merge flight id with airport code
        flight_data = [elements[0] + "_" + elements[1]]
        # Appends elements from row position 2 to flight_data
        flight_data.extend(elements[2:])
        # Convert transformed data back to string with elements separated by commas
        row = ",".join(flight_data)
        # Initialise Flight object
        flight = Flight(row)
        
        # Print Mapper Output for this row
        flights.append(flight)

    # Write each flight to a mapped_data.csv file
    with open("mapped_data.csv", "w") as f:
        for flight in flights:
            f.write(str(flight) + "\n")


def sort_mapped_data(mapped_data):
    """Sort mapped dataframe

    Args:
        mapped_data (pd.DataFrame): Mapped flight data
    """
    print("[*]\tSorter")
    order = CategoricalDtype(
        mapped_data[0].unique().sort(), 
        ordered=True
    )
    # Convert mapped_data key to categorical dtype
    mapped_data[0] = mapped_data[0].astype(order)
    # Sort mapped dataframe by key
    mapped_data.sort_values(0, inplace = True, ascending = True)
    # Save sorted dataframe to csv file
    mapped_data.to_csv("sorted_dataframe.csv", index=False, header = False)


def reduce(sorted_data):
    """Condense flight_id

    Args:
        sorted_data (pd.DataFrame): mapped data
    """
    print("[*]\tReduce")
    last_flight = None
    last_flight_key = None
    reduced_data = []

    # Dataframe to string of comma separated values
    sorted_data_strings = sorted_data.to_string(
        header=False, index=False, index_names=False
    ).split("\n")
    sorted_data_strings = [','.join(ele.split()) for ele in sorted_data_strings]

    # For flight in mapped data
    for line in sorted_data_strings:
        # Get flight from line
        flight = Flight(line)
        # Check if last flight is the current flight.key
        if last_flight_key != flight.get_key():
            # If last_flight_key is not current.key, output the previous key
            if last_flight_key:
                # Output to stdout
                reduced_data.append(last_flight)

            # Set new key
            last_flight = flight
            last_flight_key = flight.get_key()

        else:
            # NOTE: If last flight key is the current flight key, continue adding to passenger_list
            # Append each passenger in this flight's passenger list to last_flight passenger list
            for passenger in flight.passenger_list:
                last_flight.add_passenger(passenger)
            
    # Output Last Flight
    if last_flight_key == flight.get_key():
        reduced_data.append(last_flight)
    
    # Write reduced data to file
    with open("reduced.csv", "w") as f:
        for flight in reduced_data:
            f.write(str(flight)+"\n")


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
    with open("from_count.csv", "w") as f:
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
    with open("passengers.csv", "w") as f:
        for key, value in passengers:
            f.write(f"{key},{value}\n")
    return max_passenger, flight_count


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


def load_reduced_data():
    """Load in reduced data from csv file
    Returns:
        pd.DataFrame: reduced data
    """
    reduced_data = []
    with open("reduced.csv", "r") as f:
        data = f.readlines()

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
    mapper(passenger_data)
    # Load flights into dataframe
    flights = pd.read_csv("mapped_data.csv",header=None)
    # Sort mapped data by (flight id/airport) key
    sort_mapped_data(flights)
    # Load sorted dataframe into dataframe
    sorted_data = pd.read_csv("sorted_dataframe.csv",header=None)
    # Reduce dataframe
    reduce(sorted_data)
    # Read reduced flight
    reduced_data = load_reduced_data()

    airport_data = pd.read_csv(
        f"{data_dir}/Top30_airports_LatLong.csv",
        names=["airport", "airport_code", "lat", "long"],
    )

    airports = get_airports(airport_data)

    from_count = get_total_number_of_flights_from_airport(reduced_data, airports)
    print(*from_count, sep = "\n")

    max_passenger, flight_count = get_passenger_with_most_flights(reduced_data)
    print(f"{max_passenger} has had the most number of flights ({flight_count})")


if __name__ == "__main__":
    main()
