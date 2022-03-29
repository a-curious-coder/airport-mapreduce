#!/usr/bin/env python3
""" Reduce sorted mapped data """
import sys
import pandas as pd
from flight import Flight


def _reduce(sorted_flights, file_name="reduced_data.csv", hadoop_mode=False):
    """Condense flight_id

    Args:
        sorted_data (pd.DataFrame): mapped data
    """
    # print("[*]\tReduce")
    last_flight = None
    last_flight_key = None
    reduced_data = []

    # Dataframe to string of comma separated values
    sorted_flights = sorted_flights.to_string(
        header=False, index=False, index_names=False
    ).split("\n")
    sorted_flights = [','.join(flight.split()) for flight in sorted_flights]

    # For flight in mapped data
    for line in sorted_flights:
        # Get flight from line
        flight = Flight(line)
        # Check if last flight is the current flight.key
        if last_flight_key != flight.get_flight_key():
            # If last_flight_key is not current.key, output the previous key
            if last_flight_key:
                # Output to stdout
                reduced_data.append(last_flight)
            # Set new key
            last_flight = flight
            last_flight_key = flight.get_flight_key()
        else:
            # If last flight key is the current flight key
            #   add passenger to passenger_list
            for passenger in flight.passenger_list:
                last_flight.add_passenger(passenger)

    # Output Last Flight
    if last_flight_key == flight.get_flight_key():
        reduced_data.append(last_flight)
    # Write reduced data to file
    with open(file_name, "w", encoding="utf-8") as file:
        for flight in reduced_data:
            file.write(str(flight)+"\n")

    if hadoop_mode:
        # Write reduced data to stdout
        print(*reduced_data, sep="\n")


def main():
    """Main function"""
    # Read system input of file to dataframe
    data = pd.DataFrame(data=sys.stdin.read().splitlines())
    # Execute mapper
    _reduce(data, hadoop_mode=True)


if __name__ == "__main__":
    main()
