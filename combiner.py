"""Combiner"""
import multiprocessing


def combine(data, ret=None, procnum=None, hadoop_mode=False):
    """ Combines two partitions/lists of data into a single partition/list
    NOTE: this function is only called by multithread_combine
    Args:
        data (list): list of two lists containing flight data
        ret (list): list of lists we are writing combined data to
        procnum (int): index of ret / thread number
    """
    print("[*]\tCombine\tThread " + str(procnum))
    if hadoop_mode:
        # Write reduced data to stdout
        print(*[item for sublist in data for item in sublist], sep="\n")
    else:
        # Assign 1D list to ret at index procnum
        ret[procnum] = [item for sublist in data for item in sublist]


def multithread_combine(data, ret):
    """ Multithreaded combine function
    Args:
        data (list): list of lists
        ret (list): list of lists
    """
    jobs = []
    # Calculate list index values
    lists = zip(range(0, len(ret)*2, 2), range(1, len(ret)*2+1, 2))
    # Split data into condensed partitions
    parts = [data[s[0]:s[1]+1] for s in lists]
    # For each part in parts
    for procnum, data in enumerate(parts):
        # x = lambda data, ret, procnum : ret[procnum] = [item for sublist in data for item in sublist]
        p = multiprocessing.Process(target=combine, args=(data, ret, procnum))
        # p = multiprocessing.Process(target=combine, args=(data, ret, procnum))
        jobs.append(p)
        p.start()

    for p in jobs:
        p.join()

    for p in jobs:
        p.terminate()


def main():
    # TODO: Implement combine function to be compatible with actual hadoop
    # combine(hadoop_mode=True)
    pass


if __name__ == "__main__":
    main()
