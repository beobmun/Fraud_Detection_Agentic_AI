import os
import pandas as pd
import psycopg2
import time
import sys
from dotenv import load_dotenv
from simulator import Simulator
from rdb_admin import RDBAdmin
from generator import Generator

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("SIMULATOR_USER"),
    "password": os.getenv("SIMULATOR_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

DATA_DIR = 'data'   # docker 컨테이너 내부에서의 경로

if __name__ == "__main__":
    print(DB_CONFIG)

    # simulator = Simulator(DB_CONFIG)
    # simulator.load_static_data()
    # simulator.run_simulation()
    generator = (Generator(DATA_DIR)
                 .load_data()
                 .preprocess_data())
    rdb_admin = (RDBAdmin(DB_CONFIG)
                 .connect_db()
                 .init_schema()
                 .insert_users_data(generator.get_users())
                 .insert_merchants_data(generator.get_merchants()))
    
    while True:
        event_type, event_data = generator.get_event()
        if event_type is None:
            print("All events processed. Exiting.")
            break
        
        if event_type == "card_issue":
            rdb_admin.insert_card_data(event_data)
        elif event_type == "transaction":
            rdb_admin.insert_transaction_data(event_data)
        else:
            print(f"Unknown event type: {event_type}", flush=True)