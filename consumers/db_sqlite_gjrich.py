"""
db_sqlite_gjrich.py 

Has the following functions:
- init_db(config): Initialize the SQLite database and create the 'streamed_messages' table if it doesn't exist.
- insert_message(message, config): Insert a single processed message into the SQLite database.

Example JSON message with letter counts
Example JSON message
{"message": "I just proclaimed a king\u2019s decree! It was enchanting.",
 "author": "Beowulf", "timestamp": "2025-02-23 12:13:27",
   "category": "rule", "sentiment": 0.72,
     "keyword_mentioned": "a king\u2019s decree",
    "a": 5, "b": 0, ..., "z": 1
}
"""

#####################################
# Import Modules
#####################################

# import from standard library
import os
import pathlib
import sqlite3

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger

#####################################
# Define Function to Initialize SQLite Database
#####################################

def init_db(db_path: pathlib.Path):
    """
    Initialize the SQLite database -
    if it doesn't exist, create the 'streamed_messages' table
    and if it does, recreate it with 26 additional columns for letter counts.

    Args:
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info("Calling SQLite init_db() with {db_path=}.")
    try:
        # Ensure the directories for the db exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("SUCCESS: Got a cursor to execute SQL.")

            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")

            # Generate column definitions for a-z
            letter_columns = ", ".join(f"{chr(i)} INTEGER DEFAULT 0" for i in range(ord('a'), ord('z') + 1))
            
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    author TEXT,
                    timestamp TEXT,
                    category TEXT,
                    sentiment REAL,
                    keyword_mentioned TEXT,
                    message_length INTEGER,
                    {letter_columns}
                )
                """
            )
            conn.commit()
        logger.info(f"SUCCESS: Database initialized and table ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize a sqlite database at {db_path}: {e}")

#####################################
# Define Function to Insert a Processed Message into the Database
#####################################

def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Insert a single processed message into the SQLite database, including letter counts.

    Args:
    - message (dict): Processed message with letter counts to insert.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info("Calling SQLite insert_message() with:")
    logger.info(f"{message=}")
    logger.info(f"{db_path=}")

    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            # Prepare the placeholders for all columns, including a-z
            columns = (
                "message, author, timestamp, category, sentiment, keyword_mentioned, message_length, " +
                ", ".join(chr(i) for i in range(ord('a'), ord('z') + 1))
            )
            placeholders = ", ".join(["?"] * (7 + 26))  # 7 original + 26 letters
            values = (
                message["message"],
                message["author"],
                message["timestamp"],
                message["category"],
                message["sentiment"],
                message["keyword_mentioned"],
                message["message_length"],
            ) + tuple(message.get(chr(i), 0) for i in range(ord('a'), ord('z') + 1))
            
            cursor.execute(
                f"""
                INSERT INTO streamed_messages ({columns})
                VALUES ({placeholders})
                """,
                values,
            )
            conn.commit()
        logger.info("Inserted one message into the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into the database: {e}")

#####################################
# Define Function to Delete a Message from the Database
#####################################

def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """
    Delete a message from the SQLite database by its ID.

    Args:
    - message_id (int): ID of the message to delete.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM streamed_messages WHERE id = ?", (message_id,))
            conn.commit()
        logger.info(f"Deleted message with id {message_id} from the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from the database: {e}")

#####################################
# Define main() function for testing
#####################################
def main():
    logger.info("Starting db testing.")

    # Use config to make a path to a parallel test database
    DATA_PATH: pathlib.Path = config.get_base_data_path()
    TEST_DB_PATH: pathlib.Path = DATA_PATH / "test_buzz.sqlite"

    # Initialize the SQLite database by passing in the path
    init_db(TEST_DB_PATH)
    logger.info(f"Initialized database file at {TEST_DB_PATH}.")

    test_message = {
        "message": "I just shared a meme! It was amazing.",
        "author": "Charlie",
        "timestamp": "2025-01-29 14:35:20",
        "category": "humor",
        "sentiment": 0.87,
        "keyword_mentioned": "meme",
        "message_length": 42,
        "a": 5, "b": 0, "c": 0, "d": 1, "e": 4, "f": 0, "g": 1, "h": 1, "i": 2,
        "j": 1, "k": 0, "l": 0, "m": 4, "n": 1, "o": 0, "p": 0, "q": 0, "r": 1,
        "s": 3, "t": 2, "u": 1, "v": 0, "w": 1, "x": 0, "y": 0, "z": 1
    }

    insert_message(test_message, TEST_DB_PATH)

    # Retrieve the ID of the inserted test message
    try:
        with sqlite3.connect(TEST_DB_PATH, timeout=1.0) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM streamed_messages WHERE message = ? AND author = ?",
                (test_message["message"], test_message["author"]),
            )
            row = cursor.fetchone()
            if row:
                test_message_id = row[0]
                # Delete the test message
                delete_message(test_message_id, TEST_DB_PATH)
            else:
                logger.warning("Test message not found; nothing to delete.")
    except Exception as e:
        logger.error(f"ERROR: Failed to retrieve or delete test message: {e}")

    logger.info("Finished testing.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()