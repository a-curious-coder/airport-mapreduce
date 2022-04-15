#!/usr/bin/env python3
""" Reduce sorted mapped data """
import sys
import pandas as pd
import multiprocessing
from flight import Flight


def _reduce(sorted_flights, ret=None, procnum=-1, file_name="reduced_data.csv", hadoop_mode=False):
    """Condense flight_id

    Args:
        sorted_data (pd.DataFrame): mapped data
    """
    print("[*]\tReduce")
    last_flight = None
    last_flight_key = None
    reduced_data = []

    # If we have a list of flight objects
    if type(sorted_flights[0]) == Flight:
        # Convert each flight to string format
        flights = [str(flight) for flight in sorted_flights]
    # Else if there is no return list
    elif ret is None:
        print(sorted_flights.head())
        # Dataframe to string of comma separated values
        flights = sorted_flights.to_string(
            header=False, index=False, index_names=False
        ).split("\n")
        flights = [','.join(flight.split()) for flight in flights]

    # For flight in mapped data
    for line in flights:
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

    if ret is None:
        # Write reduced data to file
        with open(file_name, "w", encoding="utf-8") as file:
            for flight in reduced_data:
                # flight.passenger_list = list(set(flight.passenger_list))
                file.write(str(flight)+"\n")

        passengers = []
        for flight in reduced_data:
            passengers.extend(iter(flight.passenger_list))
        # Count unique passengers
        print(f"[*]\tOverall Passengers: {len(passengers)}")

        if hadoop_mode:
            # Write reduced data to stdout
            print(*reduced_data, sep="\n")
    else:
        ret[procnum] = reduced_data


def multithread_reduce(mapped_data, reduce_results):
    jobs = []

    for i, partition in enumerate(mapped_data):
        p = multiprocessing.Process(target=_reduce, args=(partition, reduce_results, i))
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
