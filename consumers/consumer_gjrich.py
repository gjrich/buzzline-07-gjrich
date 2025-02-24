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
import time
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from collections import deque

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
    Layout: 2x3 grid with CPU, RAM, Network, Processes, Read/Write, Disk.
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

    # Initialize data structures
    cpu_history = []
    ram_history = []
    net_history = []
    read_history = []
    write_history = []
    current_disk_space = 0
    top_ram_processes = []
    top_cpu_processes = []

    # Set up 2x3 figure
    fig, ((ax1, ax2, ax3), (ax4, ax5, ax6)) = plt.subplots(2, 3, figsize=(15, 10))
    plt.ion()

    plt.tight_layout(pad=2.0)
    fig.subplots_adjust(hspace=0.5, wspace=0.3)

    try:
        for message in consumer:
            message_data = message.value
            cpu = message_data['cpu_consumption']           # % (0-100, scaled)
            ram = message_data['ram_consumption']           # %
            read = message_data['read']                     # MB/s
            write = message_data['write']                   # MB/s
            disk_space = message_data['disk_space_consumption']  # %
            net = message_data['network_usage']             # KB/s
            top_ram_processes = message_data['top_ram_processes']
            top_cpu_processes = message_data['top_cpu_processes']

            now = time.time()

            # Update histories
            cpu_history.append((now, cpu))
            ram_history.append((now, ram))
            net_history.append((now, net))
            read_history.append((now, read))
            write_history.append((now, write))
            current_disk_space = disk_space

            # Trim to last 60 seconds for line charts
            cutoff = now - 60
            cpu_history = [(t, v) for t, v in cpu_history if t > cutoff]
            ram_history = [(t, v) for t, v in ram_history if t > cutoff]
            net_history = [(t, v) for t, v in net_history if t > cutoff]

            # Trim read/write to last 200 points
            if len(read_history) > 200:
                read_history = read_history[-200:]
                write_history = write_history[-200:]

            # Extract for plotting
            cpu_times, cpu_values = zip(*cpu_history) if cpu_history else ([], [])
            ram_times, ram_values = zip(*ram_history) if ram_history else ([], [])
            net_times, net_values = zip(*net_history) if net_history else ([], [])
            read_times, read_values = zip(*read_history) if read_history else ([], [])
            write_times, write_values = zip(*write_history) if write_history else ([], [])

            # Reverse time scale (60 on left, 0 on right)
            cpu_seconds = [-(t - now) for t in cpu_times] if cpu_times else [0]
            ram_seconds = [-(t - now) for t in ram_times] if ram_times else [0]
            net_seconds = [-(t - now) for t in net_times] if net_times else [0]

            # CPU Line Chart
            ax1.clear()
            ax1.plot(cpu_seconds, cpu_values, label='CPU (%)', color='blue')
            cpu_max = min(max(cpu_values, default=0) * 1.5, 100)
            ax1.set_ylim(0, cpu_max if cpu_max > 0 else 1)
            ax1.set_xlim(60, 0)
            ax1.set_title('CPU Consumption')
            ax1.set_xlabel('Seconds')
            ax1.set_ylabel('%')

            # RAM Line Chart
            ax2.clear()
            ax2.plot(ram_seconds, ram_values, label='RAM (%)', color='orange')
            ram_max = min(max(ram_values, default=0) * 1.5, 100)
            ax2.set_ylim(0, ram_max if ram_max > 0 else 1)
            ax2.set_xlim(60, 0)
            ax2.set_title('RAM Consumption')
            ax2.set_xlabel('Seconds')
            ax2.set_ylabel('%')

            # Network Line Chart
            ax3.clear()
            ax3.plot(net_seconds, net_values, label='Network (KB/s)', color='purple')
            net_max = max(net_values, default=0) * 1.5
            ax3.set_ylim(0, net_max if net_max > 0 else 1)
            ax3.set_xlim(60, 0)
            ax3.set_title('Network Usage')
            ax3.set_xlabel('Seconds')
            ax3.set_ylabel('KB/s')

            # Top 5 Processes (from Kafka data)
            ax4.clear()
            ram_text = "\n".join([f"{p['name']}: {p['ram']:.1f}%" for p in top_ram_processes])
            cpu_text = "\n".join([f"{p['name']}: {p['cpu']:.1f}%" for p in top_cpu_processes])
            ax4.text(0.1, 0.95, "Top 5 RAM\n" + ram_text, va='top', fontsize=8)
            ax4.text(0.6, 0.95, "Top 5 CPU\n" + cpu_text, va='top', fontsize=8)
            ax4.axis('off')
            ax4.set_title('Top Processes')

            # Read vs Write Scatter
            ax5.clear()
            ax5.scatter(read_values, write_values, color='green', s=10)
            all_values = read_values + write_values
            max_value = max(all_values, default=0) * 1.5
            ax5.set_xlim(-max_value * 0.1, max_value)
            ax5.set_ylim(-max_value * 0.1, max_value)
            ax5.set_xticks([0] + [i for i in ax5.get_xticks() if i > 0])
            ax5.set_yticks([0] + [i for i in ax5.get_yticks() if i > 0])
            ax5.set_title('Read vs Write')
            ax5.set_xlabel('Read (MB/s)')
            ax5.set_ylabel('Write (MB/s)')

            # Disk Space Pie Chart
            ax6.clear()
            ax6.pie(
                [current_disk_space, 100 - current_disk_space],
                labels=['Consumed', 'Empty'],
                autopct='%1.1f%%',
                colors=['red', 'lightgray']
            )
            ax6.set_title('Disk Space Consumption')

            plt.draw()
            plt.pause(6)  # Increased to 6s for less lag

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
    consumer = None
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