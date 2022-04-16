"""Flight object"""


class Flight:
    """Flight class
    Attributes:
        flight_id (str): flight id
        from_airport (str): from airport
        to_airport (str): to airport
        depart_time (str): departure time
        total_flight_time (int): total flight time
        passenger_list (list): list of passengers
        """

    def __init__(self, flight_data):
        """Initialise flight object

        Args:
            flight_data (list): list of flight dat
        """
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
            self.get_flight_key(),
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

    def __len__(self):
        """Returns length of passengers for this flight

        Returns:
            int: length of passengers
        """
        return 5 + len(self.passenger_list)

    def add_passenger(self, passenger):
        """Add passenger to flight

        Returns:
            list: updated passenger list
        """
        # If passenger already in self.passenger_list, return self.passenger_list
        if passenger in self.passenger_list:
            return self.passenger_list
        self.passenger_list.append(passenger)
        return self.passenger_list

    def get_flight_key(self):
        """get key for flight data

        Returns:
            str: flight key
        """
        key = self.flight_id + "_" + self.from_airport
        return key


def main():
    """Main function to test flight class"""
    flight = Flight("SQU6245R_DEN,FRA,1420564460,1049,UES9151GS5")
    print(flight)
    flight = Flight("SQU6245R_DEN,FRA,1420564460,1049,test1,test2,test3")
    print(flight)


if __name__ == "__main__":
    main()
