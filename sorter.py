#!/usr/bin/env python3
"""Sorter function for mapped data"""
import sys
import pandas as pd
from pandas.api.types import CategoricalDtype


def _sort(mapped_data, file_name="sorted_data.csv", hadoop_mode=False):
    """Sort mapped dataframe

    Args:
        mapped_data (pd.DataFrame): Mapped flight data
    """
    # print("[*]\tSorter")
    order = CategoricalDtype(
        mapped_data[0].unique().sort(),
        ordered=True
    )
    # Convert mapped_data key to categorical dtype
    mapped_data[0] = mapped_data[0].astype(order)
    # Sort mapped dataframe by key
    mapped_data.sort_values(0, inplace=True, ascending=True)
    # Save sorted dataframe to csv file
    mapped_data.to_csv(file_name, index=False, header=False)
    if hadoop_mode:
        # Write sorted dataframe to stdout
        print(mapped_data.to_string(index=False, header=False))


def main():
    """Main function"""
    # Read system input of file to dataframe
    data = pd.DataFrame(data=sys.stdin.read().splitlines())
    # Execute mapper
    _sort(data, hadoop_mode=True)


if __name__ == "__main__":
    main()
