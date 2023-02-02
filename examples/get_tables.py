import logging
import os

from ai_common_utils.files import load_env_file
from ai_postgres_client import PostgresClient


logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)
if not os.environ.get("DOCKER_ENV"):
    load_env_file(".env")


os.environ["POSTGRES_HOST"] = "localhost"


pc = PostgresClient()
pc.connect()

print(pc.get_tables())
