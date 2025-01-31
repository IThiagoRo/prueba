import os
import requests
import pandas as pd
from dataclasses import dataclass

@dataclass
class DataIngestionConfig:
    db_path = os.path.join('data/', 'db.csv')
    api_url = "https://l2h237eh53.execute-api.us-east-1.amazonaws.com/dev/precios"

class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()
        self.db = None

    def export_to_csv(self):
        self.db = pd.DataFrame(self.db)
        self.db.to_csv(self.ingestion_config.db_path, index=True)

    def connect_to_api(self, start_date='2024-03-15', end_date='2024-04-14'):
        params = {'start_date': start_date,
                  'end_date' : end_date
                  }
        response = requests.get(self.ingestion_config.api_url, params=params)

        if response.status_code == 200:
            data = response.json()
            self.db = data['data']
        else:
            print(f"Error connect to api {reponse.status_code}, {response.text}")


    def initiate_data_ingestion(self):
        print("Initate Data Ingestion Process")
        self.connect_to_api()
        print("Api Data to csv...")
        self.export_to_csv()
