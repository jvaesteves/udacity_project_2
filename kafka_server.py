from pathlib import Path
from producer_server import ProducerServer


def run_kafka_server():
    input_file = f"{Path(__file__).parents[0]}/police-department-calls-for-service.json"

    producer = ProducerServer(
        input_file=input_file,
        topic="POLICE_CALLS",
        bootstrap_servers="localhost:9092"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
