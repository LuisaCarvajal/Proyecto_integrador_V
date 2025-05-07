from collector import SamsungDataCollector

if __name__ == "__main__":
    collector = SamsungDataCollector()
    data = collector.fetch_data()
    collector.update_sqlite(data)
    collector.update_csv(data)
