import time
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
            
            # Intentar descargar los datos
            result = None
            attempts = 0
            while attempts < 5 and result is None:
                result = self._attempt_download()
                if result is None:
                    attempts += 1
                    self.logger.warning(f"Intento {attempts} fallido. Reintentando en 60 segundos...")
                    time.sleep(60)  # Espera 60 segundos antes de reintentar

            if result is None:
                self.logger.error("No se pudo obtener los datos después de varios intentos.")
                return pd.DataFrame()

            df = result

            # Validar si es un DataFrame (normal) o un tuple (inconsistente)
            if isinstance(df, tuple):
                self.logger.warning("Se recibió una tupla de yf.download, extrayendo primer elemento.")
                df = df[0]  # usar solo el DataFrame

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

    def _attempt_download(self):
        try:
            result = yf.download(self.ticker, period="max", progress=False)
            if isinstance(result, tuple):
                return result
            return result
        except Exception as e:
            self.logger.error(f"Error al intentar descargar datos: {e}")
            return None

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
                    self.logger.warning("Como la tabla no existe, se creará una nueva.")

                merged.to_sql("samsung_data", conn, if_exists="replace", index=False)
                self.logger.info(f"SQLite actualizado. Total: {len(merged)} registros.")
        except Exception as e:
            self.logger.error(f"Error al actualizar SQLite: {e}")

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
                self.logger.warning("No existe un archivo CSV, se creará uno nuevo.")

            merged.to_csv(self.csv_path, index=False)
            self.logger.info(f"CSV actualizado. Total: {len(merged)} registros.")
        except Exception as e:
            self.logger.error(f"Error al actualizar el CSV: {e}")
