import sqlite3
import pandas as pd

class Output:
    def __init__(self):
        self.db = None
        self.conn = None

    def export_to_db(self):
        self.db.to_sql('PlusenergyTable', self.conn, if_exists='replace', index=False)

    def connect_to_db(self):
        self.conn = sqlite3.connect('precios.db')

    def initiate_output(self):
        print('Initiate Output Process...')
        self.connect_to_db()
        print('Data Frame to sqlite DB...')
        self.export_to_db()
