"""
producer_gjrich.py

Stream JSON data to a file and - if available - a Kafka topic.

Example JSON message
{
    "cpu_consumption": 25.3,            # % (0-100, system-wide)
    "ram_consumption": 42.0,            # % (0-100)
    "read": 10.8,                       # MB/s
    "write": 9.5,                       # MB/s
    "disk_space_consumption": 30.1,     # % (0-100 for C:)
    "network_usage": 150.7,             # KB/s (sent + received)
    "top_ram_processes": [{"name": "chrome.exe", "ram": 5.2}, ...],
    "top_cpu_processes": [{"name": "python.exe", "cpu": 10.1}, ...]  # % (0-100, system-wide)
}
"""

#####################################
# Import Modules
#####################################

import json
import os
import pathlib
import sys
import time
from datetime import datetime

from kafka import KafkaProducer
import psutil

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
    Generate a stream of JSON messages with real Windows system metrics using psutil.
    Units: CPU %, RAM %, read MB/s, write MB/s, disk % (C:), network KB/s, plus top processes.
    CPU scaled to system-wide 0-100%.
    """
    # Get number of logical CPUs once (for process scaling)
    num_cpus = psutil.cpu_count()

    while True:
        # CPU usage (%) over a 1-second interval, system-wide
        cpu_percent = psutil.cpu_percent(interval=1, percpu=False)  # Average across all cores

        # RAM usage (%) based on total available memory
        ram_info = psutil.virtual_memory()
        ram_percent = ram_info.percent

        # Disk I/O (read/write in MB/s over 1 second)
        io_before = psutil.disk_io_counters()
        time.sleep(1)
        io_after = psutil.disk_io_counters()
        read_mb_s = (io_after.read_bytes - io_before.read_bytes) / (1024 ** 2)
        write_mb_s = (io_after.write_bytes - io_before.write_bytes) / (1024 ** 2)

        # Network usage (sent + received in KB/s over 1 second)
        net_before = psutil.net_io_counters()
        time.sleep(1)
        net_after = psutil.net_io_counters()
        net_bytes = (net_after.bytes_sent + net_after.bytes_recv) - (net_before.bytes_sent + net_before.bytes_recv)
        net_kb_s = net_bytes / 1024

        # Disk space usage (%) for C: drive
        disk_space_percent = psutil.disk_usage('C:\\').percent

        # Top 5 processes by RAM and CPU
        processes = []
        for proc in psutil.process_iter(['pid', 'name', 'memory_percent', 'cpu_percent']):
            try:
                cpu_scaled = proc.info['cpu_percent'] / num_cpus  # Scale to system-wide 0-100%
                processes.append({
                    'name': proc.info['name'],
                    'ram': proc.info['memory_percent'],
                    'cpu': cpu_scaled
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        top_ram_processes = sorted(processes, key=lambda x: x['ram'], reverse=True)[:5]
        top_cpu_processes = sorted(processes, key=lambda x: x['cpu'], reverse=True)[:5]

        # Create JSON message
        json_message = {
            "cpu_consumption": round(cpu_percent, 1),
            "ram_consumption": round(ram_percent, 1),
            "read": round(read_mb_s, 1),
            "write": round(write_mb_s, 1),
            "disk_space_consumption": round(disk_space_percent, 1),
            "network_usage": round(net_kb_s, 1),
            "top_ram_processes": [{'name': p['name'], 'ram': round(p['ram'], 1)} for p in top_ram_processes],
            "top_cpu_processes": [{'name': p['name'], 'cpu': round(p['cpu'], 1)} for p in top_cpu_processes]
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
        interval_secs: int = config.get_message_interval_seconds_as_int()  # Now 2
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

            if producer:
                producer.send(topic, value=message)
                logger.info(f"STEP 4b Sent message to Kafka topic '{topic}': {message}")

            time.sleep(interval_secs)  # 2 seconds now

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