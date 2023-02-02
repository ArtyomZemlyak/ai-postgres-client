import logging
import os

from ai_common_utils.files import load_env_file
from ai_postgres_client import PostgresClient


logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)
if not os.environ.get("DOCKER_ENV"):
    load_env_file(".env")


pc = PostgresClient()
pc.connect()

print(pc.get_tables())

columns = """
id INTEGER PRIMARY KEY,
name VARCHAR(5) NOT NULL,
mytext text NOT NULL
"""

pc.create_table("my_table", columns=columns)
pc.create_table("my_table", columns=columns)
print(pc.get_tables())

pc.add_row("my_table", [11, "12345", "HELLO!"])
print(pc.find_value("my_table", "id", 11))

pc.remove_row("my_table", "id", 11)
print(pc.find_value("my_table", "id", 11))

pc.remove_table("my_table")
print(pc.get_tables())
