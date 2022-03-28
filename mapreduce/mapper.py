#!/usr/bin/env python3
""" Mapper function """
import sys
import pandas as pd
from mapreduce.flight import Flight


def _map(data, file_name="mapped_data.csv", hadoop=False):
    """Reformat and map flight data
    Args:
        data (pd.Dataframe): passenger data with headers
    """

    # print("[*]\tMapper")
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

    # Write each flight to a mapped_data.csv file
    with open(file_name, "w", encoding="utf-8") as file:
        for flight in flights:
            file.write(str(flight) + "\n")
    if hadoop:
        # Write each flight to stdout
        print(*flights, sep="\n")


def main():
    """Main function"""
    # Read system input of file to dataframe
    data = pd.DataFrame(data=sys.stdin.read().splitlines())
    # Execute mapper
    _map(data, hadoop=True)


if __name__ == "__main__":
    main()
