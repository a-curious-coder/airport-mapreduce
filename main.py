import os
import time
import pandas as pd
import geopy.distance
from dotenv import load_dotenv
from datetime import datetime

def get_flights_per_airport(airport_data, passenger_data):
    """Gets the number of flights from each airport

    Args:
        data (pd.DataFrame): Passenger flight data

    Returns:
        list, list: list of tuples cotaining airport counts, list of unused airports
    """
    # print("#airports\tdataset")
    # print(f"{len(airport_data['airport'].unique())}\t\tAirport")
    # print(f"{len(passenger_data['from_airport'].unique())}\t\tPassenger")
    start = time.perf_counter()
    counts = passenger_data['from_airport'].value_counts()
    airports = counts.index.tolist()
    airports = zip(airports, counts)
    airport_counts = [i for i in airports]
    
    passenger_airports = passenger_data['from_airport'].unique()
    unused = airport_data[airport_data.airport_code.isin(passenger_airports)]
    unused_airports = unused['airport_code'].tolist()
    # print("----------\nAirport Counts\n-----------")
    # print(airport_counts)
    # print("----------\nUnused Airports\n-----------")
    # print(unused_airports)
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
    flight_ids = passenger_data['flight_id'].unique()
    flights = []
    for flight in flight_ids:
        # Passenger list per flight
        this_flight = passenger_data[passenger_data['flight_id'] == flight]
        passengers = this_flight['passenger_id'].unique()
        # From_airport code
        airport_code = this_flight['from_airport'].unique()
        #   departure time (HH:MM:SS)
        departure_times = this_flight['departure_time'].unique()
        cdeparture_times = [datetime.fromtimestamp(dtime).strftime("%H:%M:%S") for dtime in this_flight['departure_time'].unique()]
        #   arrival time (HH:MM:SS))
        flight_durations = [duration * 60 for duration in this_flight['flight_duration'].unique()]
        arrival_times = [departure_time + duration for departure_time, duration in zip(departure_times, flight_durations)]
        carrival_times = [datetime.fromtimestamp(dtime).strftime("%H:%M:%S") for dtime in arrival_times]
        #   flight times
        flight_times = [datetime.fromtimestamp(dtime).strftime("%H:%M:%S") for dtime in flight_durations]
        temp = [flight, passengers, airport_code, cdeparture_times, carrival_times, flight_times]
        flights.append(temp)
    end = time.perf_counter()
    print(f"[2]\tExecution Time: {end-start:.2f}s")
    return flights


def get_highest_airmile_passenger(airport_data, passenger_data):
    """ Calculates miles travelled for each passenger based on flight data

    Args:
        airport_data (pd.DataFrame): Airport data
        passenger_data (pd.DataFrame): Passenger flight data

    Returns:
        string: id of passenger with greatest distance travelled
    """
    start = time.perf_counter()
    # Nautical miles for each flight
    # get list of flights to and from
    flights = passenger_data[['passenger_id', 'from_airport', 'to_airport']]
    # create dictionary for each airport mapped to its coordinates
    locations = [(lat, long) for lat, long in zip(airport_data['lat'], airport_data['long'])]
    # Map airport to lat/long location
    airport_dict = dict(zip(airport_data['airport_code'], locations))
    # print(*airport_dict.items(), sep = "\n")

    distances = []
    # Replace all airports with their corresponding coordinates
    for index, flight in flights.iterrows():
        # Convert from_airport values to corresponding coordinates in dictionary
        flight['from_airport'] = airport_dict[flight['from_airport']]
        # Convert to_airport values to corresponding coordinates in dictionary
        flight['to_airport'] = airport_dict[flight['to_airport']]
        # Calculate distance in nautical miles to 2dp
        distance = round(geopy.distance.distance(flight['from_airport'], flight['to_airport']).miles, 2)
        # Collect this flight's distance in a list
        distances.append(distance)
    
    flights['distances'] = distances    
    # Sum of distances travelled for each passenger
    passenger_distances = dict()
    for passenger in flights['passenger_id']:
        # Passenger's flights
        passenger_flights = flights[flights['passenger_id'] == passenger]
        # Sum this passenger's distances travelled
        passenger_distance = round(passenger_flights['distances'].sum(), 2)
        passenger_distances[passenger] = passenger_distance
    # print(passenger_distances.items())
    passenger = max(passenger_distances, key=passenger_distances.get)
    # print(passenger, passenger_distances[passenger])
    # Return passenger data with most airmiles
    end = time.perf_counter()
    print(f"[3]\tExecution Time: {end-start:.2f}s")
    return passenger


def main():
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
    # print(passenger_data.info())
    airport_data = pd.read_csv(
        f"{data_dir}/Top30_airports_LatLong.csv",
        names=["airport", "airport_code", "lat", "long"],
    )

    # Collect data
    flights_per_airport, unused_airports = get_flights_per_airport(airport_data, passenger_data)
    flight_list = get_flight_list(passenger_data)
    passenger = get_highest_airmile_passenger(airport_data, passenger_data)
    
    # Print data
    print("-------------------------")
    print("Flight List")
    print("-------------------------")
    print(flights_per_airport)

    print("-------------------------")
    print("Flights per airport")
    print("-------------------------")
    print(flight_list[:1])

    print("-------------------------")
    print("Unused airports")
    print("-------------------------")
    print(unused_airports)

    print("-------------------------")
    print("Highest Airmile passenger")
    print("-------------------------")
    print(passenger)

if __name__ == "__main__":
    main()
