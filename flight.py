class Flight:
    """Flight class"""

    def __init__(self, flight_data):
        (
            flight_from,
            self.to_airport,
            self.depart_time,
            self.total_flight_time,
            self.passenger_list,
        ) = flight_data.strip().split(",", 4)

        self.flight_id, self.from_airport = flight_from.split("_")
        self.passenger_list = list(set(self.passenger_list.strip().split(",")))

    def __str__(self):
        """Converts each element in this flight object to a string and returns
        it as string

        Returns:
            list: list of flight object elements as string
        """

        # Create list with flight attribute values
        results = [
            self.get_key(),
            self.to_airport,
            self.depart_time,
            str(self.total_flight_time),
        ]

        # If passenger_list has more than 1 passenger, extend to results
        if len(self.passenger_list) > 1:
            results.extend(self.passenger_list)
        else:
            # Otherwise, just add to results
            results += self.passenger_list
        # List to tab separated string
        return ",".join(results)
        # return str(results)

    def __len__(self):
        return 5 + len(self.passenger_list)
    
    def add_passenger(self, passenger):
        """Add passenger to flight

        Returns:
            list: updated passenger list
        """
        self.passenger_list.append(passenger)
        return self.passenger_list

    def get_key(self):
        """get key for flight data

        Returns:
            str: key
        """
        key = self.flight_id + "_" + self.from_airport
        return key

    def get_clean_passenger_list(self):
        """Removes duplicate passengers

        Returns:
            list: clean passenger list
        """
        return list(set(self.passenger_list))
