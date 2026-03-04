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

    generator = (Generator(DATA_DIR)
                 .load_data()
                 .preprocess_data())
    rdb_admin = (RDBAdmin(DB_CONFIG)
                 .connect_db()
                 .init_schema()
                 .insert_users_data(generator.get_users())
                 .insert_merchants_data(generator.get_merchants()))
    total_transactions = len(generator.get_transactions())

    print("Streaming events...", flush=True, end="")
    while True:
        event_type, event_data = generator.get_event()
        if event_type is None:
            print("All events processed. Exiting.")
            rdb_admin.close_connection()
            break
        
        # 이벤트 타입에 따라 적절한 DB(PostgreSQL) 삽입 함수 호출
        if event_type == "card_issue":
            rdb_admin.insert_card_data(event_data)
        elif event_type == "transaction":
            rdb_admin.insert_transaction_data(event_data)
            print(f"\rStreamed transaction event: {generator.current_transaction_idx} / {total_transactions}", flush=True, end="")
        else:
            print(f"Unknown event type: {event_type}", flush=True)
        
        # Ontology_Admin을 통한 온톨로지 저장 (추후 구현 예정)

        # 다음 event 발생 신호 대기 