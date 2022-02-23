class Flight:
    """Flight class"""

    def __init__(self, file):
        delimiter = None
        flight_from = None

        # If it is csv,
        if "," in file:
            delimiter = ","
        else:
            # Otherwise, it should be tsv
            delimiter = "\t"

        (
            flight_from,
            self.to_airport,
            self.depart_time,
            self.total_flight_time,
            self.passenger_list,
        ) = file.strip().split(delimiter, 4)

        self.flight_id, self.from_airport = flight_from.split("_")
        self.passenger_list = list(set(self.passenger_list.strip().split(delimiter)))

    def to_string(self):
        """Converts each element in this flight object to a string and returns it as string

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
            results.extend(set(self.passenger_list))
        else:
            # Otherwise, just add to results
            results += self.passenger_list

        return results

    def get_key(self):
        """get key for flight

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

    def __len__(self):  # For len()
        return 5 + len(self.passenger_list)
