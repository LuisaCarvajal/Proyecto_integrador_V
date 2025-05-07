import yfinance as yf
import pandas as pd
import sqlite3
from logger import setup_logger
import os

class SamsungDataCollector:
    def __init__(self, ticker="005930.KS", db_path="src/piv/static/data/historical.db", csv_path="src/piv/static/data/historical.csv"):
        self.ticker = ticker
        self.db_path = db_path
        self.csv_path = csv_path
        self.logger = setup_logger()
        self.logger.info(f"Iniciando recolección de datos para {self.ticker}")

    def fetch_data(self):
            try:
                self.logger.info("Descargando los datos desde Yahoo Finance.")
                result = yf.download(self.ticker, period="max", progress=False)

                 # Validar si es un DataFrame (normal) o un tuple (inconsistente)
                if isinstance(result, tuple):
                     self.logger.warning("Se recibió una tupla de yf.download, extrayendo primer elemento.")
                     df = result[0]  # usar solo el DataFrame
                else:
                    df = result

                if df.empty:
                    self.logger.warning("No se descargaron datos.")
                    return pd.DataFrame()

                df.reset_index(inplace=True)
                df.columns = [str(col).lower().replace(" ", "_") for col in df.columns]
                self.logger.info(f"{len(df)} filas descargadas.")
                return df

            except Exception as e:
                self.logger.error(f"Error al descargar los datos: {e}")
                return pd.DataFrame()

    def update_sqlite(self, df):
        if df.empty:
            self.logger.warning("No hay datos para guardar en SQLite.")
            return

        self.logger.info("Conectando a SQLite.")
        try:
            with sqlite3.connect(self.db_path) as conn:
                try:
                    existing = pd.read_sql("SELECT * FROM samsung_data", conn)
                    merged = pd.concat([existing, df]).drop_duplicates(subset="date")
                    self.logger.info("Hay datos existentes. Fusionando...")
                except Exception:
                    merged = df
                    self.logger.warning("Como la tabla no existe se creara una nueva.")

                merged.to_sql("samsung_data", conn, if_exists="replace", index=False)
                self.logger.info(f"SQLite actualizado. Total: {len(merged)} registros.")
        except Exception as e:
            self.logger.error(f"Error al actualizar el SQLite: {e}")

    def update_csv(self, df):
        if df.empty:
            self.logger.warning("No hay datos para guardar en el CSV.")
            return

        try:
            if os.path.exists(self.csv_path):
                existing = pd.read_csv(self.csv_path)
                merged = pd.concat([existing, df]).drop_duplicates(subset="date")
                self.logger.info("Hay un CSV existente. Fusionando.")
            else:
                merged = df
                self.logger.warning("No hay creado un archivo CSV, se creara uno nuevo.")

            merged.to_csv(self.csv_path, index=False)
            self.logger.info(f"CSV actualizado. Total: {len(merged)} registros.")
        except Exception as e:
            self.logger.error(f"Error al actualizar el CSV: {e}")