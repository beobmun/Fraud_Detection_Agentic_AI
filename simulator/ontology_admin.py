import pandas as pd
import ast
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD

class OntologyAdmin:
    def __init__(self):
        self.g = Graph()

        self.FDA = Namespace("http://www.fraud-detection.ai/")
        self.GEO = Namespace("http://www.fraud-detection.ai/geo#")

        self.g.bind("fda", self.FDA)
        self.g.bind("geo", self.GEO)

        self.added_uris = set()

        print("OntologyAdmin initialized", flush=True)

    def init_schema(self):
        print("Initializing ontology schema...", flush=True)

        # Define classes
        self.g.add((self.FDA.User, RDF.type, RDFS.Class))
        self.g.add((self.FDA.Card, RDF.type, RDFS.Class))
        self.g.add((self.FDA.Merchant, RDF.type, RDFS.Class))
        self.g.add((self.FDA.Transaction, RDF.type, RDFS.Class))

        self.g.add((self.GEO.Nation, RDF.type, RDFS.Class))
        self.g.add((self.GEO.State, RDF.type, RDFS.Class))
        self.g.add((self.GEO.City, RDF.type, RDFS.Class))

        # Define subclasses of Transaction
        self.g.add((self.FDA.Purchase, RDF.type, RDFS.Class))
        self.g.add((self.FDA.Purchase, RDFS.subClassOf, self.FDA.Transaction))

        self.g.add((self.FDA.Refund, RDF.type, RDFS.Class))
        self.g.add((self.FDA.Refund, RDFS.subClassOf, self.FDA.Transaction))

        # Define Object properties
        self.g.add((self.FDA.own, RDF.type, RDF.Property))
        self.g.add((self.FDA.own, RDFS.domain, self.FDA.User))
        self.g.add((self.FDA.own, RDFS.range, self.FDA.Card))

        self.g.add((self.FDA.usedIn, RDF.type, RDF.Property))
        self.g.add((self.FDA.usedIn, RDFS.domain, self.FDA.Card))
        self.g.add((self.FDA.usedIn, RDFS.range, self.FDA.Transaction))

        self.g.add((self.FDA.occurredAt, RDF.type, RDF.Property))
        self.g.add((self.FDA.occurredAt, RDFS.domain, self.FDA.Transaction))
        self.g.add((self.FDA.occurredAt, RDFS.range, self.FDA.Merchant))

        self.g.add((self.GEO.liveIn, RDF.type, RDF.Property))
        self.g.add((self.GEO.liveIn, RDFS.domain, self.FDA.User))

        self.g.add((self.GEO.locatedIn, RDF.type, RDF.Property))
        self.g.add((self.GEO.locatedIn, RDFS.domain, self.FDA.Merchant))

        self.g.add((self.GEO.partOf, RDF.type, RDF.Property))

        print("Ontology schema initialized successfully.", flush=True)
        return self
    
    def insert_users_data(self, users: pd.DataFrame):
        print("Inserting users data into ontology...", flush=True)

        nation_uri = self.GEO["Nation_USA"]
        self.g.add((nation_uri, RDF.type, self.GEO.Nation))
        self.added_uris.add(nation_uri)

        for _, user in users.iterrows():
            user_dict = {k: (None if pd.isna(v) else v) for k, v in user.to_dict().items()}
            
            user_uri = self.FDA[f"User_{user_dict['UUID']}"]
            self.g.add((user_uri, RDF.type, self.FDA.User))
            
            city = user_dict.get('City')
            state = user_dict.get('State')
            
            if state and city:
                state_uri = self.GEO[f"State_{state.replace(' ', '_')}"]
                city_uri = self.GEO[f"City_{city.replace(' ', '_')}_{state.replace(' ', '_')}"]

                if state_uri not in self.added_uris:
                    self.g.add((state_uri, RDF.type, self.GEO.State))
                    self.g.add((state_uri, self.GEO.partOf, nation_uri))
                    self.added_uris.add(state_uri)
                if city_uri not in self.added_uris:
                    self.g.add((city_uri, RDF.type, self.GEO.City))
                    self.g.add((city_uri, self.GEO.partOf, state_uri))
                    self.added_uris.add(city_uri)

            self.g.add((user_uri, self.GEO.liveIn, city_uri))

            if user_dict.get('Birth'):
                self.g.add((user_uri, self.FDA.hasBirth, Literal(user_dict['Birth'], datatype=XSD.date)))
            if user_dict.get('Gender'):
                self.g.add((user_uri, self.FDA.hasGender, Literal(user_dict['Gender'], datatype=XSD.string)))
            if user_dict.get('Per Capita Income - Zipcode'):
                self.g.add((user_uri, self.FDA.hasPerCapitaIncomeZipcode, Literal(user_dict['Per Capita Income - Zipcode'], datatype=XSD.float)))
            if user_dict.get('Yearly Income - Person'):
                self.g.add((user_uri, self.FDA.hasYearlyIncomePerson, Literal(user_dict['Yearly Income - Person'], datatype=XSD.float)))
            if user_dict.get('Total Debt'):
                self.g.add((user_uri, self.FDA.hasTotalDebt, Literal(user_dict['Total Debt'], datatype=XSD.float)))
            if user_dict.get('FICO Score'):
                self.g.add((user_uri, self.FDA.hasFICOScore, Literal(user_dict['FICO Score'], datatype=XSD.integer)))
            
        print("Users data inserted into ontology successfully.", flush=True)
        return self
    
    def insert_merchants_data(self, merchants: pd.DataFrame):
        print("Inserting merchants data into ontology...", flush=True)

        for _, merchant in merchants.iterrows():
            merchant_dict = {k: (None if pd.isna(v) else v) for k, v in merchant.to_dict().items()}
            
            merchant_uri = self.FDA[f"Merchant_{merchant_dict['UUID']}"]
            self.g.add((merchant_uri, RDF.type, self.FDA.Merchant))
            
            state = merchant_dict.get('State')
            city = merchant_dict.get('City')
            
            if city:
                if state is None:
                    city_uri = self.GEO[f"City_{city.replace(' ', '_')}"]
                    if city_uri not in self.added_uris:
                        self.g.add((city_uri, RDF.type, self.GEO.City))
                        self.added_uris.add(city_uri)
                    self.g.add((merchant_uri, self.GEO.locatedIn, city_uri))
                elif len(str(state)) == 2:
                    nation = "USA"
                    nation_uri = self.GEO[f"Nation_{nation}"]
                    if nation_uri not in self.added_uris:
                        self.g.add((nation_uri, RDF.type, self.GEO.Nation))
                        self.added_uris.add(nation_uri)
                    state_uri = self.GEO[f"State_{state.replace(' ', '_')}"]
                    if state_uri not in self.added_uris:
                        self.g.add((state_uri, RDF.type, self.GEO.State))
                        self.g.add((state_uri, self.GEO.partOf, nation_uri))
                        self.added_uris.add(state_uri)
                    city_uri = self.GEO[f"City_{city.replace(' ', '_')}_{state.replace(' ', '_')}"]
                    if city_uri not in self.added_uris:
                        self.g.add((city_uri, RDF.type, self.GEO.City))
                        self.g.add((city_uri, self.GEO.partOf, state_uri))
                        self.added_uris.add(city_uri)
                    self.g.add((merchant_uri, self.GEO.locatedIn, city_uri))
                else:
                    nation_uri = self.GEO[f"Nation_{state.replace(' ', '_')}"]
                    if nation_uri not in self.added_uris:
                        self.g.add((nation_uri, RDF.type, self.GEO.Nation))
                        self.added_uris.add(nation_uri)
                    city_uri = self.GEO[f"City_{city.replace(' ', '_')}_{state.replace(' ', '_')}"]
                    if city_uri not in self.added_uris:
                        self.g.add((city_uri, RDF.type, self.GEO.City))
                        self.g.add((city_uri, self.GEO.partOf, nation_uri))
                        self.added_uris.add(city_uri)
                    self.g.add((merchant_uri, self.GEO.locatedIn, city_uri))
            
            if merchant_dict.get('MCC') is not None:
                self.g.add((merchant_uri, self.FDA.hasMCC, Literal(int(merchant_dict['MCC']), datatype=XSD.integer)))
            if merchant_dict.get('Name') is not None:
                self.g.add((merchant_uri, self.FDA.hasName, Literal(str(merchant_dict['Name']), datatype=XSD.string)))
            
        print("Merchants data inserted into ontology successfully.", flush=True)
        return self
    
    def insert_card_data(self, data: dict):
        card_uri = self.FDA[f"Card_{data['UUID']}"]
        user_uri = self.FDA[f"User_{data['User']}"]

        self.g.add((card_uri, RDF.type, self.FDA.Card))
        self.g.add((user_uri, self.FDA.own, card_uri))

        if data.get('Card Brand') is not None:
            self.g.add((card_uri, self.FDA.hasCardBrand, Literal(str(data['Card Brand']), datatype=XSD.string)))
        if data.get('Card Type') is not None:
            self.g.add((card_uri, self.FDA.hasCardType, Literal(str(data['Card Type']), datatype=XSD.string)))
        if data.get('Expires') is not None:
            dt_str = pd.to_datetime(data['Expires']).isoformat()
            self.g.add((card_uri, self.FDA.hasExpires, Literal(dt_str, datatype=XSD.dateTime)))
        if data.get('Has Chip') is not None:
            self.g.add((card_uri, self.FDA.hasChip, Literal(bool(data['Has Chip']), datatype=XSD.boolean)))
        if data.get('Cards Issued') is not None:
            self.g.add((card_uri, self.FDA.hasCardsIssued, Literal(int(data['Cards Issued']), datatype=XSD.integer)))
        if data.get('Credit Limit') is not None:
            self.g.add((card_uri, self.FDA.hasCreditLimit, Literal(float(data['Credit Limit']), datatype=XSD.float)))
        if data.get('Acct Open Date') is not None:
            acct_open_date_str = pd.to_datetime(data['Acct Open Date']).strftime('%Y-%m-%d')
            self.g.add((card_uri, self.FDA.hasAcctOpenDate, Literal(acct_open_date_str, datatype=XSD.date)))
        if data.get('Year PIN last Changed') is not None:
            self.g.add((card_uri, self.FDA.hasYearPINLastChanged, Literal(int(data['Year PIN last Changed']), datatype=XSD.integer)))
        if data.get('Card on Dark Web') is not None:
            self.g.add((card_uri, self.FDA.hasCardOnDarkWeb, Literal(bool(data['Card on Dark Web']), datatype=XSD.boolean)))

    def insert_transaction_data(self, data: dict):
        transaction_uri = self.FDA[f"Transaction_{data['UUID']}"]
        card_uri = self.FDA[f"Card_{data['Card']}"]
        merchant_uri = self.FDA[f"Merchant_{data['Merchant']}"]

        # Amount가 음수이면 Refund, 양수이면 Purchase로 간주
        if data.get('Amount') is not None:
            amount = float(data['Amount'])
            if amount >= 0:
                self.g.add((transaction_uri, RDF.type, self.FDA.Purchase))
            else:
                self.g.add((transaction_uri, RDF.type, self.FDA.Refund))
            self.g.add((transaction_uri, self.FDA.hasAmount, Literal(abs(amount), datatype=XSD.float)))
        else:
            self.g.add((transaction_uri, RDF.type, self.FDA.Transaction))
        
        self.g.add((card_uri, self.FDA.usedIn, transaction_uri))
        self.g.add((transaction_uri, self.FDA.occurredAt, merchant_uri))

        if data.get('Datetime') is not None:
            dt_str = pd.to_datetime(data['Datetime']).isoformat()
            self.g.add((transaction_uri, self.FDA.hasDatetime, Literal(dt_str, datatype=XSD.dateTime)))
        if data.get('Use Chip') is not None:
            self.g.add((transaction_uri, self.FDA.hasUseChip, Literal(str(data['Use Chip']), datatype=XSD.string)))
        if data.get('Errors') and data['Errors'] != '':
            try:
                errors_list = ast.literal_eval(str(data['Errors']))
            except (ValueError, SyntaxError):
                errors_list = [e.strip() for e in str(data['Errors']).split(',') if e.strip()]
            for error in errors_list:
                self.g.add((transaction_uri, self.FDA.hasError, Literal(error, datatype=XSD.string)))

    def save_to_file(self, file_path: str):
        self.g.serialize(destination=file_path, format='turtle')
        print(f"Ontology saved to {file_path}", flush=True)

    def close_connection(self, file_path: str):
        self.save_to_file(file_path)