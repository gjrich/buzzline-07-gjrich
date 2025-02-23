"""
consumer_gjrich.py

Consume JSON messages from a Kafka topic and visualize computing resource consumption using Matplotlib.
"""

#####################################
# Import Modules
#####################################

import json
import os
import pathlib
import sys
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from collections import deque

# Import specific functions instead of 'config'
from utils.utils_config import (
    get_kafka_topic,
    get_kafka_broker_address,
    get_kafka_consumer_group_id
)
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

#####################################
# Consume Messages from Kafka Topic
#####################################

def consume_messages_from_kafka(topic: str, kafka_url: str, group: str):
    """
    Consume new messages from Kafka topic and update visualizations.
    Each message is expected to be JSON-formatted with resource consumption data.

    Args:
    - topic (str): Kafka topic to consume messages from.
    - kafka_url (str): Kafka broker address.
    - group (str): Consumer group ID for Kafka.
    """
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   {topic=}")
    logger.info(f"   {kafka_url=}")
    logger.info(f"   {group=}")

    logger.info("Step 1. Verify Kafka Services.")
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(11)

    logger.info("Step 2. Create a Kafka consumer.")
    consumer = None
    try:
        consumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    logger.info("Step 3. Verify topic exists.")
    if consumer is not None:
        try:
            is_topic_available(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.error(
                f"ERROR: Topic '{topic}' does not exist. Please run the Kafka producer. : {e}"
            )
            sys.exit(13)

    logger.info("Step 4. Process messages.")
    if consumer is None:
        logger.error("ERROR: Consumer is None. Exiting.")
        sys.exit(13)


    # Initialize data structures for visualizations
    cpu_history = deque(maxlen=30)  # Last 30 snapshots for CPU
    ram_history = deque(maxlen=30)  # Last 30 snapshots for RAM
    read_history = []              # Accumulate all read values
    write_history = []             # Accumulate all write values
    current_disk_space = 0         # Latest disk space value

    # Set up Matplotlib figure with 2x2 subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 10))  # Increased figure size slightly
    plt.ion()  # Enable interactive mode for live updates

    # Apply tight layout for autosizing and spacing
    plt.tight_layout(pad=2.0)  # Add padding between subplots
    fig.subplots_adjust(hspace=0.5, wspace=0.3)

    try:
        for message in consumer:
            # Extract resource values from the Kafka message
            message_data = message.value
            cpu = message_data['cpu_consumption']           # Unit: % (1 to 100)
            ram = message_data['ram_consumption']           # Unit: GB (1 to 16)
            read = message_data['read']                     # Unit: MB/s (1 to 100)
            write = message_data['write']                   # Unit: MB/s (1 to 100)
            disk_space = message_data['disk_space_consumption']  # Unit: % (10 to 100)

            # Convert RAM from GB to percentage (assuming total RAM is 16 GB)
            ram_percent = (ram / 16) * 100

            # Update data structures
            cpu_history.append(cpu)
            ram_history.append(ram_percent)
            read_history.append(read)
            write_history.append(write)
            current_disk_space = disk_space

            # Update CPU Consumption Line Chart
            ax1.clear()
            ax1.plot(range(len(cpu_history)), cpu_history, label='CPU (%)', color='blue')
            # Set dynamic y-axis max (50% more than highest value in last 30 steps, minimum 0)
            cpu_max = max(cpu_history) if cpu_history else 0
            ax1.set_ylim(0, cpu_max * 1.5 if cpu_max > 0 else 1)  # Ensure at least 1 for visibility
            ax1.set_title('CPU Consumption')
            ax1.set_xlabel('Time Steps')
            ax1.set_ylabel('%')

            # Update RAM Consumption Line Chart
            ax2.clear()
            ax2.plot(range(len(ram_history)), ram_history, label='RAM (%)', color='orange')
            # Set dynamic y-axis max (50% more than highest value in last 30 steps, minimum 0)
            ram_max = max(ram_history) if ram_history else 0
            ax2.set_ylim(0, ram_max * 1.5 if ram_max > 0 else 1)  # Ensure at least 1 for visibility
            ax2.set_title('RAM Consumption')
            ax2.set_xlabel('Time Steps')
            ax2.set_ylabel('%')

            # Update Read vs Write Scatter Plot
            ax3.clear()
            ax3.scatter(read_history, write_history, color='green', s=10)
            # Set dynamic axes max (50% more than highest of read or write, minimum 1)
            all_values = read_history + write_history
            max_value = max(all_values) if all_values else 1
            ax3.set_xlim(1, max_value * 1.5)  # Read range
            ax3.set_ylim(1, max_value * 1.5)  # Write range (same max for symmetry)
            ax3.set_title('Read vs Write')
            ax3.set_xlabel('Read (MB/s)')
            ax3.set_ylabel('Write (MB/s)')

            # Update Disk Space Pie Chart
            ax4.clear()
            ax4.pie(
                [current_disk_space, 100 - current_disk_space],
                labels=['Consumed', 'Empty'],
                autopct='%1.1f%%',
                colors=['red', 'lightgray']
            )
            ax4.set_title('Disk Space Consumption')

            # Refresh the plot
            plt.draw()
            plt.pause(3)  # Brief pause to update the display


    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise
    finally:
        if consumer is not None:
            consumer.close()
            logger.info("Kafka consumer closed cleanly.")

#####################################
# Define Main Function
#####################################

def main():
    """
    Main function to run the consumer process.

    Reads configuration and starts consumption and visualization.
    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        topic = get_kafka_topic()
        kafka_url = get_kafka_broker_address()
        group_id = get_kafka_consumer_group_id()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Begin consuming and visualizing messages.")
    consumer = None  # Track consumer for cleanup
    try:
        consumer = consume_messages_from_kafka(topic, kafka_url, group_id)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if consumer is not None and hasattr(consumer, 'close'):
            consumer.close()
            logger.info("Kafka consumer closed cleanly.")
        logger.info("Consumer shutting down.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()