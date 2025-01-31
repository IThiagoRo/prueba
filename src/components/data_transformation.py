import pandas as pd
from dataclasses import dataclass

@dataclass
class DataTransformationConfig:
    db_path = 'data/db.csv'

class DataTransformation:
    def __init__(self):
        self.data_transformation_config = DataTransformationConfig()
        self.db = None
        self.new_db = None

    def calculate_avg(self):
        daily_avg = self.db.groupby("fecha")["precio"].mean().reset_index()
        last_7_days_avg = daily_avg["precio"].rolling(window=7, min_periods=1).mean()

        new_df = pd.DataFrame()
        new_df['fecha'] = daily_avg.fecha
        new_df['precio_promedio'] = daily_avg.precio
        new_df['precio_7d'] = last_7_days_avg

        self.new_db = new_df

    def fill_missing_with_avg(self, df):
        for i in range(len(df)):
            if pd.isna(df.loc[i, "precio"]):
                current_date = df.loc[i, "fecha"]
                # Se toman los 3 dias previos y los 3 dias posteriores
                prev_days = df[(df["fecha"] < current_date) & (df["fecha"] >= current_date - pd.Timedelta(days=3))]["precio"]
                next_days = df[(df["fecha"] > current_date) & (df["fecha"] <= current_date + pd.Timedelta(days=3))]["precio"]

                avg_price = pd.concat([prev_days, next_days]).mean()

                if not pd.isna(avg_price):
                    df.loc[i, "precio"] = avg_price
        return df

    def initiate_data_transformation(self):
        print("Initiate Data Transformation ...")
        self.db = self.db.reset_index()
        self.db = self.db.rename(columns={'index': 'hora'})
        self.db = pd.melt(self.db, id_vars=['hora'], var_name='fecha', value_name='precio')
        self.db.hora = self.db.hora.str.replace('24', '00', regex=True)
        self.db.hora = pd.to_datetime(self.db.hora, format='%H:%M').dt.strftime("%H:%M")


        print("NA Process...")
        self.db["precio"] = self.db["precio"].fillna(method="ffill")
        self.db = self.fill_missing_with_avg(self.db)

        print("AVG Process...")
        self.calculate_avg()

