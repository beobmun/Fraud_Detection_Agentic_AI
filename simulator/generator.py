import pandas as pd
import numpy as np
import os

DATA_DIR = 'data'   # docker 컨테이너 내부에서의 경로

class Generator:
    def __init__(self, data_dir=DATA_DIR):
        self.data_dir = data_dir

        self.transactions = None
        self.users = None
        self.cards = None
        self.merchants = None

        self.current_card_idx = 0
        self.current_transaction_idx = 0
        print("Generator initialized", flush=True)

    def load_data(self):
        self.transactions = pd.read_csv(os.path.join(self.data_dir, 'transactions.csv'))
        print("Transactions loaded:", self.transactions.shape, flush=True)
        
        self.users = pd.read_csv(os.path.join(self.data_dir, 'users.csv'))
        print("Users loaded:", self.users.shape, flush=True)
        
        self.cards = pd.read_csv(os.path.join(self.data_dir, 'cards.csv'))
        print("Cards loaded:", self.cards.shape, flush=True)
        
        self.merchants = pd.read_csv(os.path.join(self.data_dir, 'merchants.csv'))
        print("Merchants loaded:", self.merchants.shape, flush=True)

        return self
    
    def preprocess_data(self):
        # 날짜 컬럼을 datetime으로 변환
        self.transactions['Datetime'] = pd.to_datetime(self.transactions['Datetime'], errors='coerce')
        self.cards['Expires'] = pd.to_datetime(self.cards['Expires'], errors='coerce', format='mixed') + pd.offsets.MonthEnd(0)
        self.cards['Expires'] = self.cards['Expires'].dt.normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        self.cards['Acct Open Date'] = pd.to_datetime(self.cards['Acct Open Date'], errors='coerce', format='mixed')
        self.cards['Acct Open Date'] = self.cards['Acct Open Date'].dt.normalize()

        self.transactions = self.transactions.sort_values(by='Datetime').reset_index(drop=True)
        self.cards = self.cards.sort_values(by='Acct Open Date').reset_index(drop=True)

        self.merchants['Zipcode'] = pd.to_numeric(self.merchants['Zipcode'], errors='coerce').astype('Int64')
        
        return self
    
    def get_event(self):
        current_card_date = self.cards.loc[self.current_card_idx, 'Acct Open Date'] if self.current_card_idx < len(self.cards) else pd.Timestamp.max
        current_transaction_date = self.transactions.loc[self.current_transaction_idx, 'Datetime'] if self.current_transaction_idx < len(self.transactions) else pd.Timestamp.max

        if current_card_date == pd.Timestamp.max and current_transaction_date == pd.Timestamp.max:
            return None, None # 모든 이벤트가 처리된 경우

        if current_card_date <= current_transaction_date:
            event = self.cards.iloc[self.current_card_idx]
            self.current_card_idx += 1
            return "card_issue", {k: (None if pd.isna(v) else v) for k, v in event.to_dict().items()}
        
        else:
            event = self.transactions.iloc[self.current_transaction_idx]
            self.current_transaction_idx += 1
            return "transaction", {k: (None if pd.isna(v) else v) for k, v in event.to_dict().items()}

    def get_transactions(self):
        if self.transactions is None:
            raise ValueError("Data not loaded. Call load_data() first.")
        return self.transactions

    def get_users(self):
        if self.users is None:
            raise ValueError("Data not loaded. Call load_data() first.")
        return self.users
    
    def get_cards(self):
        if self.cards is None:
            raise ValueError("Data not loaded. Call load_data() first.")
        return self.cards
    
    def get_merchants(self):
        if self.merchants is None:
            raise ValueError("Data not loaded. Call load_data() first.")
        return self.merchants
