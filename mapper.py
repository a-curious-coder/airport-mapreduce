#! /usr/bin/env python
"""mapper.py"""

from __future__ import print_function
import sys
import re
import pandas as pd
import Levenshtein
from flight import Flight


def get_airports(data):
    """Data Wrangling to match airport_code to corresponding airport/lat/long

    Args:
        data (pd.DataFrame): Airport data

    Returns:
        dict: airport and corresponding airports' data
    """

    airports = []
    for _, row in data.iterrows():
        airports.append(row["airport_code"])
    return airports


# Global variables
FID_POOL = []
PID_POOL = []

airport_data = pd.read_csv(
    "data/Top30_airports_LatLong.csv",
    names=["airport", "airport_code", "lat", "long"],
)
# Import airport list for checking airport code
airport_list = get_airports(airport_data)
# airport_list = get_airports(airport_data)


# Read line by line in STDIN
for line in sys.stdin:
    # print(type(line))
    # for line in airport_data.iterrows():
    # Remove white Space & # Split into words by ","
    elements = line.strip().split(",")

    # Skip if the length of the line is not 6 elements
    if len(elements) != 6:
        continue

    # Error Detection & Correction
    # 1. NULL RECORD
    # If any of the element is empty, skip
    result = any([re.search("^\s*$", x) for x in elements])
    if result:
        print("%s\t%s" % ("NULL_RECORD_DISCARD", 1))
        continue

    # Reconstruct the line for data structure
    # Put Passenger ID at the last place of the list
    elements = elements[-5:] + elements[:-5]
    # Combine Flight ID & Departure Aiport Code
    results = [elements[0] + "_" + elements[1]]
    results.extend(elements[2:])
    line = "\t".join(results)
    current = Flight(line)

    # Error Detection & Correction
    # 2. IATA/FAA Code Format
    if current.from_airport not in airport_list:
        # Fit the cloest AIPORT if it is not on the list
        current.FROM_AIRPORT = Levenshtein.distance(current.FROM_AIRPORT, airport_list)
        print("%s\t%s" % ("FROM_AIRPORT_FORMAT_ERROR", 1))

    if current.to_airport not in airport_list:
        # Fit the cloest AIPORT if it is not on the list
        current.TO_AIRPORT = Levenshtein.distance(current.TO_AIRPORT, airport_list)
        print("%s\t%s" % ("TO_AIRPORT_FORMAT_ERROR", 1))

    # Mapper Output
    results = current.output()
    print("\t".join(results))

    # Add FLIGHT_ID & PASSENGER_ID TO THE POOL
    FID_POOL.append(current.get_key())
    PID_POOL.append(current.passenger_list[0])

# Output ID_POOL for cross-referencing
print("0_flight_pool", "\t", "", sep="", end="")
print("\t".join(list(set(FID_POOL))))
print("0_passenger_pool", "\t", "", sep="", end="")
print("\t".join(list(set(PID_POOL))))
