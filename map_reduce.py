import threading
from collections import defaultdict


class Mapper():
    def __init__(self):
        self.mapout_blocks = []  # Store the output blocks from each thread
        self.lock = threading.Lock()  # Lock for thread-safe operations

    def map_data_block(self, flight_data_block):
        output = defaultdict(list)  # Use defaultdict with list to store flight info per passenger

        for line in flight_data_block:
            data = line.strip().split(",")
            Passenger_id = data[0].strip()
            flight_id = data[1].strip()
            Departure_time = data[4].strip()

            # Add flight info for each passenger, avoid duplicates
            if Passenger_id not in output:
                output[Passenger_id] = [(flight_id, Departure_time)]
            else:
                if (flight_id, Departure_time) not in output[Passenger_id]:
                    output[Passenger_id].append((flight_id, Departure_time))

        # Append the output block safely with locking
        with self.lock:
            self.mapout_blocks.append(output)

    def parallel_mapper(self, flight_data, num_threads):
        block_size = len(flight_data) // num_threads  # Calculate data block size for each thread
        threads = []

        for i in range(num_threads):
            start_index = i * block_size
            end_index = (i + 1) * block_size if i < num_threads - 1 else len(flight_data)
            data_block = flight_data[start_index:end_index]

            # Start a thread for each data block
            thread = threading.Thread(target=self.map_data_block, args=(data_block,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

    def get_mapper_out(self):
        return self.mapout_blocks  # Return all the map outputs


class Reducer():
    def __init__(self):
        self.result_container = defaultdict(list)  # Store the total flight count per passenger
        self.lock = threading.Lock()  # Lock for thread-safe operations
        self.result = defaultdict(int)

    def reduce_data_chunk(self, reducer_block):

        for passenger_id, flights in reducer_block.items():
            if passenger_id not in self.result_container:
                self.result_container[passenger_id] = flights
            else:
                for flight in flights:
                    if flight not in self.result_container[passenger_id]:
                        self.result_container[passenger_id].append(flight)

    def process_result_chunk(self, items):
        for passenger_id, flights in items:
            with self.lock:
                self.result[passenger_id] = len(flights)

    def parallel_reducer(self, reducer_blocks,num_threads=4):
        threads = []

        # Start a thread to process each reducer block
        for reducer_block in reducer_blocks:
            self.reduce_data_chunk(reducer_block)
        items = list(self.result_container.items())
        chunk_size = len(items) // num_threads

        for i in range(num_threads):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i < num_threads - 1 else len(items)
            chunk = items[start:end]
            thread = threading.Thread(target=self.process_result_chunk, args=(chunk,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Find the maximum flight count
        max_value = max(self.result.values())

        # Find all passengers who have the maximum flight count
        passengers = [key for key, value in self.result.items() if value == max_value]
        return passengers, max_value


def read_data(file_path):
    # Read the flight data file and return as a list of lines
    with open(file_path, 'r') as file:
        return file.readlines()


def main():
    # Load flight data
    flight_data = read_data('./Cloud Computing/Task data/AComp_Passenger_data_no_error.csv')

    # Initialize and run the Mapper
    mapper = Mapper()
    mapper.parallel_mapper(flight_data, 4)
    map_output = mapper.get_mapper_out()

    # Initialize and run the Reducer
    reducer = Reducer()
    max_passenger, flight_count = reducer.parallel_reducer(map_output)

    # Output the final results
    print(f"Passengers with the most flights: {max_passenger}")
    print(f"Number of flights: {flight_count}")


if __name__ == "__main__":
    main()
