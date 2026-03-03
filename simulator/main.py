import os
import pandas as pd
import psycopg2
import time
import sys
from dotenv import load_dotenv
from simulator import Simulator

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("SIMULATOR_USER"),
    "password": os.getenv("SIMULATOR_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}


if __name__ == "__main__":

    print(DB_CONFIG)

    simulator = Simulator(DB_CONFIG)
    simulator.load_static_data()
    simulator.run_simulation()