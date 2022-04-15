#!/usr/bin/env python3
"""Sorter function for mapped data"""
import sys
import pandas as pd
from pandas.api.types import CategoricalDtype


def _sort(data, file_name="sorted_data.csv", hadoop_mode=False, ret=None, procnum=-1):
    """Sort mapped dataframe

    Args:
        data (pd.DataFrame): Mapped flight data
    """
    flights = list({entry.split(',')[0] for entry in data[0].values})
    # print("[*]\tSorter")
    order = CategoricalDtype(
        flights.sort(),
        ordered=True
    )

    # Convert data key to categorical dtype
    data[0] = data[0].astype(order)
    # Sort mapped dataframe by key
    data.sort_values(0, inplace=True, ascending=True)
    
    if hadoop_mode:
        # Write sorted dataframe to stdout
        print(data.to_string(index=False, header=False))
    # print(f"Sorted Length: {len(data)}")
    if ret is not None:
        ret[procnum] = data
    else:
        # Save sorted dataframe to csv file
        data.to_csv(file_name, index=False, header=False)

    return data


def main():
    """Main function"""
    # Read system input of file to dataframe
    data = pd.DataFrame(data=sys.stdin.read().splitlines())
    # Execute mapper
    _sort(data, hadoop_mode=True)


if __name__ == "__main__":
    main()
