#!/usr/bin/env python3
"""Sorter function for mapped data"""
import sys
import flight
import multiprocessing
import pandas as pd
from pandas.api.types import CategoricalDtype

def _sort(data, ret=None, procnum=None, file_name="mapreduce_output/sorted_data.csv", hadoop_mode=False):
    """Sort list of flight objects in alphabetical order

    Args:
        data (list): Mapped flight partition
    """
    # If there's no processor number, it's a single threaded call
    if procnum is None:
        print("[*]\tSingle Thread Sort")
        # 2D list to 1D list
        data = [item for sublist in data for item in sublist]

    # If there's a valid processor number
    if type(procnum) == int:
        print("[*]\tSorter\tThread " + str(procnum))

    # get list of unique flight ids from flights in data
    # Combine each list in reduce_results
    data = [str(item) for item in data]
    # Convert data to dataframe
    data = pd.DataFrame(data)

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

    # Convert sorted  to list
    data = data[0].tolist()
    # Convert list of flight strings to flight objects
    data = [flight.Flight(row) for row in data]

    if hadoop_mode:
        # Write sorted dataframe to stdout
        print(data.to_string(index=False, header=False))

    if ret is not None:
        # Write sorted flight data to list
        ret[procnum] = data
        return

    # Save list to file
    with open(file_name, 'w') as f:
        for item in data:
            f.write(str(item) + '\n')

    return data


def multithread_sort(partitions, ret):
    """Multithreaded sort function
    Args:
        partitions (list): list of partitions
        ret (list): list of lists to assign mapped results to
    """
    jobs = []

    for i, partition in enumerate(partitions):
        p = multiprocessing.Process(target=_sort, args=(partition,
                                                        ret, i))
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
    _sort(data, hadoop_mode=True)


if __name__ == "__main__":
    main()
