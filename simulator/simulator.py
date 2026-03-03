import pandas as pd
import psycopg2
import time
from datetime import datetime
import sys
import json
import numpy as np
import os
import ast
import gc

DATA_DIR = 'data'   # docker 컨테이너 내부에서의 경로

class Simulator:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.cor = None
        self.connect_db()
        self.init_schema()

    def connect_db(self):
        retries = 5
        while retries > 0:
            try:
                print(f"Connecting to DB at {self.db_config['host']} as {self.db_config['user']}...", flush=True)
                self.conn = psycopg2.connect(**self.db_config)
                self.cur = self.conn.cursor()
                print("Connected successfully.", flush=True)
                return
            except psycopg2.OperationalError as e:
                print(f"DB Connection failed: {e}", flush=True)
                print(f"Retrying in 5 seconds... ({retries} left)", flush=True)
                time.sleep(5)
                retries -= 1
        
        print("Could not connect to Database. Exiting.", flush=True)
        sys.exit(1)
                
    def init_schema(self):
        # 테이블 스키마 생성 및 초기화
        print("Initializing database schema...", flush=True)
        try:
            # 테이블 초기화 (의존성 역순으로 DROP)
            self.cur.execute("""
                DROP TABLE IF EXISTS transactions;
                DROP TABLE IF EXISTS cards;
                DROP TABLE IF EXISTS merchants;
                DROP TABLE IF EXISTS users;

                -- 1. Users 테이블 (cards는 UUID 배열로 관리)
                CREATE TABLE users (
                    uuid UUID PRIMARY KEY,
                    retirement_age INT,
                    birth DATE,
                    gender VARCHAR(10),
                    address VARCHAR(255),
                    apartment VARCHAR(50),
                    city VARCHAR(100),
                    state VARCHAR(50),
                    zipcode VARCHAR(20),
                    per_capita_income_zipcode FLOAT,
                    yearly_income_person FLOAT,
                    total_debt FLOAT,
                    fico_score INT,
                    num_credit_cards INT DEFAULT 0,
                    cards UUID[] DEFAULT '{}'
                );

                -- 2. Merchants 테이블
                CREATE TABLE merchants (
                    uuid UUID PRIMARY KEY,
                    name VARCHAR(255),
                    city VARCHAR(100),
                    state VARCHAR(50),
                    zipcode VARCHAR(20),
                    mcc VARCHAR(10)
                );

                -- 3. Cards 테이블
                CREATE TABLE cards (
                    uuid UUID PRIMARY KEY,
                    user_uuid UUID REFERENCES users(uuid),
                    card_brand VARCHAR(50),
                    card_type VARCHAR(50),
                    card_number VARCHAR(50),
                    expires DATE,
                    cvv VARCHAR(10),
                    has_chip BOOLEAN,
                    cards_issued INT,
                    credit_limit FLOAT,
                    acct_open_date DATE,
                    year_pin_last_changed INT,
                    card_on_dark_web BOOLEAN
                );

                -- 4. Transactions 테이블 (errors는 TEXT 배열로 관리)
                CREATE TABLE transactions (
                    uuid UUID PRIMARY KEY,
                    datetime TIMESTAMP,
                    user_uuid UUID REFERENCES users(uuid),
                    card UUID REFERENCES cards(uuid),
                    merchant UUID REFERENCES merchants(uuid),
                    amount FLOAT,
                    use_chip VARCHAR(50),
                    errors TEXT[] DEFAULT '{}'
                );
                             
                -- 5. answers 테이블 (is fraud 정답지)
                CREATE TABLE answers (
                    uuid UUID PRIMARY KEY REFERENCES transactions(uuid),
                    fraud BOOLEAN
                );
            """)
            self.conn.commit()
            print("Schema initialized successfully.", flush=True)
        except Exception as e:
            print(f"Error during schema initialization: {e}", flush=True)
            self.conn.rollback()
    
    def load_static_data(self):
        # 정적 데이터 로드 (Users, Merchants)
        print("Loading static data...", flush=True)
        users_df = pd.read_csv(os.path.join(DATA_DIR, 'users.csv'))
        merchants_df = pd.read_csv(os.path.join(DATA_DIR, 'merchants.csv'))
        merchants_df['Zipcode'] = pd.to_numeric(merchants_df['Zipcode'], errors='coerce').astype('Int64')
        
        users_df = users_df.astype(object).where(pd.notna(users_df), None)
        merchants_df = merchants_df.astype(object).where(pd.notna(merchants_df), None)

        user_data = []
        for _, row in users_df.iterrows():
            user_data.append((
                row['UUID'], row['Retirement Age'], row['Birth'], row['Gender'], row['Address'], row['Apartment'], row['City'], row['State'], row['Zipcode'], row['Per Capita Income - Zipcode'], row['Yearly Income - Person'], row['Total Debt'], row['FICO Score']
            ))
        self.cur.executemany(
            """
            INSERT INTO users (uuid, retirement_age, birth, gender, address, apartment, city, state, zipcode, per_capita_income_zipcode, yearly_income_person, total_debt, fico_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, user_data
        )

        merchant_data = []
        for _, row in merchants_df.iterrows():
            merchant_data.append((
                row['UUID'], row['Name'], row['City'], row['State'], row['Zipcode'], row['MCC']
            ))
        self.cur.executemany(
            """
            INSERT INTO merchants (uuid, name, city, state, zipcode, mcc)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, merchant_data
        )

        self.conn.commit()
        print("Static data loaded successfully.", flush=True)

    def _process_card_issue(self, data):
        # cards DB에 데이터 추가
        self.cur.execute(
            """
                INSERT INTO cards (uuid, user_uuid, card_brand, card_type, card_number, expires, cvv, has_chip, cards_issued, credit_limit, acct_open_date, year_pin_last_changed, card_on_dark_web)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['UUID'], data['User'], data['Card Brand'], data['Card Type'],
                data['Card Number'], data['Expires'], str(data['CVV']), data['Has Chip'],
                data['Cards Issued'], data['Credit Limit'], data['Acct Open Date'],
                data['Year PIN last Changed'], data['Card on Dark Web']
            )
        )
        # users DB의 num_credit_cards, cards 배열 업데이트
        self.cur.execute(
            """
                UPDATE users
                SET num_credit_cards = num_credit_cards + 1,
                    cards = array_append(cards, %s::uuid)
                WHERE uuid = %s
            """, (data['UUID'], data['User'])
        )
        self.conn.commit()

    def _process_transaction(self, data):
        # Errors 리스트 처리 python list -> PostgreSQL TEXT[]
        errors_list = []
        if pd.notna(data['Errors']) and data['Errors'] != '':
            try:
                errors_list = ast.literal_eval(str(data['Errors']))
            except (ValueError, SyntaxError):
                errors_list = [e.strip() for e in str(data['Errors']).split(',')]

        self.cur.execute(
            """
                INSERT INTO transactions (uuid, datetime, user_uuid, card, merchant, amount, use_chip, errors)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['UUID'], data['Datetime'], data['User'], data['Card'],
                data['Merchant'], data['Amount'], data['Use Chip'], errors_list
            )
        )

        self.cur.execute(
            """
                INSERT INTO answers (uuid, fraud)
                VALUES (%s, %s)
            """, (
                data['UUID'], data['Fraud']
            )
        )

        self.conn.commit()

    def run_simulation(self):
        print("Loading cards data ...", flush=True)
        cards_df = pd.read_csv(os.path.join(DATA_DIR, 'cards.csv'))
        cards_df['Acct Open Date'] = pd.to_datetime(cards_df['Acct Open Date'], errors='coerce')
        cards_df = cards_df.sort_values(by='Acct Open Date').reset_index(drop=True)

        print("Processing card issuance events ...", flush=True)
        cards_list = []
        for _, row in cards_df.iterrows():
            event = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
            cards_list.append(event)

        del cards_df
        gc.collect()

        print("Start streaming transactions ...", flush=True)
        transactions_df = pd.read_csv(os.path.join(DATA_DIR, 'transactions.csv'), chunksize=10000)

        for chunk in transactions_df:
            chunk['Datetime'] = pd.to_datetime(chunk['Datetime'], errors='coerce')
            
            for _, row in chunk.iterrows():
                event = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
                trans_time = event['Datetime']
                while cards_list:
                    card_time = cards_list[0]['Acct Open Date']
                    if card_time <= trans_time:
                        self._process_card_issue(cards_list.pop(0))
                    else:
                        break
                self._process_transaction(event)
        
        while cards_list:
            self._process_card_issue(cards_list.pop(0))

        print("Simulation completed.", flush=True)
        self.cur.close()
        self.conn.close()