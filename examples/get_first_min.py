import logging
import os
from time import time

from ai_common_utils.files import load_env_file
from ai_postgres_client import PostgresClient


logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)
if not os.environ.get("DOCKER_ENV"):
    load_env_file(".env")


pc = PostgresClient()
pc.connect()


print(pc.get_first_min_row("speech", "idx", "time_processing", retun_column="idx"))


st = time()
for i in range(1000):
    pc.get_first_min_row("speech", "idx", "time_processing", retun_column="idx")
print(f"Time nonempty: {time() - st}")


st = time()
for i in range(1000):
    pc.get_first_min_row("speech_todo", "idx", "queue", retun_column="idx")
print(f"Time empty: {time() - st}")


"""
Time nonempty: 0.5107409954071045
Time empty: 0.26514697074890137
"""
