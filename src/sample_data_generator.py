from sample_publisher import Publisher
import random


if __name__ == "__main__":
    print(random.randint(1,10))

    publisher = Publisher()
    publisher.send_records(num_of_records=10000000)
