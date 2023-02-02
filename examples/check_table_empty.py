import logging
import os
from time import time

from ai_common_utils.files import load_env_file
from ai_postgres_client import PostgresClient


logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)
if not os.environ.get("DOCKER_ENV"):
    load_env_file(".env")


os.environ["POSTGRES_HOST"] = "localhost"


pc = PostgresClient()
pc.connect()

print(pc.check_table_is_empty("speech"))
print(pc.check_table_is_empty("speech_todo"))

st = time()
for i in range(1000):
    pc.check_table_is_empty("speech_todo")

print(f"Time: {time() - st}")

"""
Time: 0.1671004295349121
"""
