"""
producer_gjrich.py

Stream JSON data to a file and - if available - a Kafka topic.

Example JSON message
{
    "cpu_consumption": 25.3,
    "ram_consumption": 4.2,
    "read": 10.8,
    "write": 9.5,
    "disk_space_consumption": 30.1
}

Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime

# import external modules
from kafka import KafkaProducer

# import from local modules
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger

#####################################
# Stub Sentiment Analysis Function
#####################################

def assess_sentiment(text: str) -> float:
    """
    Stub for sentiment analysis.
    Returns a random float between 0 and 1 for now.
    """
    return round(random.uniform(0, 1), 2)

#####################################
# Define Message Generator
#####################################

def generate_messages():
    """
    Generate a stream of JSON messages with dummy computing resource consumption data.
    """
    # Define resource constraints
    RESOURCES = {
        "cpu_consumption": {"min": 1, "max": 100, "initial": 25},
        "ram_consumption": {"min": 1, "max": 16, "initial": 4},
        "read": {"min": 1, "max": 100, "initial": 10},
        "write": {"min": 1, "max": 100, "initial": 10},
        "disk_space_consumption": {"min": 10, "max": 100, "initial": 30}
    }

    # Initialize current values
    current_values = {key: info["initial"] for key, info in RESOURCES.items()}

    while True:
        # Generate new values by adjusting each resource by -10% to +10%
        for resource, info in RESOURCES.items():
            current = current_values[resource]
            # Random change between -10% and +10%
            change_percent = random.uniform(-0.15, 0.15)
            new_value = current * (1 + change_percent)

            # Enforce min/max bounds
            if new_value < info["min"]:
                new_value = info["min"]  # Stay at min if trying to go lower
            elif new_value > info["max"]:
                new_value = info["max"]  # Stay at max if trying to go higher
            
            # Round to 1 decimal place for readability
            current_values[resource] = round(new_value, 1)

        # Create JSON message with resource consumption
        json_message = {
            "cpu_consumption": current_values["cpu_consumption"],
            "ram_consumption": current_values["ram_consumption"],
            "read": current_values["read"],
            "write": current_values["write"],
            "disk_space_consumption": current_values["disk_space_consumption"]
        }

        yield json_message

#####################################
# Define Main Function
#####################################

def main() -> None:

    logger.info("Starting Producer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read required environment variables.")

    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete the live data file if exists to start fresh.")

    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")

        logger.info("STEP 3. Build the path folders to the live_data file if needed.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to delete live data file: {e}")
        sys.exit(2)

    logger.info("STEP 4. Try to create a Kafka producer and topic.")
    producer = None

    try:
        verify_services()
        producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        logger.info(f"Kafka producer connected to {kafka_server}")
    except Exception as e:
        logger.warning(f"WARNING: Kafka connection failed: {e}")
        producer = None

    if producer:
        try:
            create_kafka_topic(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.warning(f"WARNING: Failed to create or verify topic '{topic}': {e}")
            producer = None

    logger.info("STEP 5. Generate messages continuously.")
    try:
        for message in generate_messages():
            logger.info(message)

            with live_data_path.open("a") as f:
                f.write(json.dumps(message) + "\n")
                logger.info(f"STEP 4a Wrote message to file: {message}")

            # Send to Kafka if available
            if producer:
                producer.send(topic, value=message)
                logger.info(f"STEP 4b Sent message to Kafka topic '{topic}': {message}")

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("WARNING: Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("TRY/FINALLY: Producer shutting down.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()