import argparse
import requests
from datetime import datetime, timedelta
import pandas as pd
import sqlite3

# File where the DB is located
DB_NAME = "bitcoin.db"

# Function to create DB if it doesn't exist yet
def create_db():
    with sqlite3.connect(DB_NAME) as con:
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bitcoin_price (
                date TEXT PRIMARY KEY,
                price REAL NOT NULL,
                moving_average REAL
            )
        """)

# Function to retrieve the data within the given range
def get_data():
    create_db()

    # Data pull, dates are converted to unixtime
    url = """https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range?vs_currency=usd&from={start}&to={end}""".format(
            start = s.timestamp(),
            end = e.timestamp()
        )
    res = requests.get(url).json()


    # The API for ranges might give several records per day (with an hourly or minute cadence)
    # For this reason a preprocessing step is needed before storing the data    
    df = pd.DataFrame(res["prices"], columns=['date', 'price'])

    # Conversion from unixtime to timestamps
    df['date'] = pd.to_datetime(df["date"], unit="ms")

    # Delete duplicates keeping only the last record
    idx = df.date.dt.date
    df = df[~idx.duplicated(keep='last') | ~idx.duplicated(keep=False)]

    # Format the timestamp in the proper DB format
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    # Insert the data into the DB
    with sqlite3.connect(DB_NAME) as con:
        sql = "INSERT OR REPLACE INTO bitcoin_price (date, price, moving_average) VALUES "
        for _, row in df.iterrows():
            sql += """("{date}", {price}, NULL), """.format(
                date = row["date"],
                price = row["price"]
            )
            cur = con.cursor()
            cur.execute(sql[:len(sql) - 2])
            con.commit()

# Function to calculate the moving averages for the given range
def calculate_moving_average():
    with sqlite3.connect(DB_NAME) as con:

        # Retrive data for the range and 5 days before
        df = pd.read_sql("""
            SELECT 
                *
            FROM bitcoin_price
            WHERE date BETWEEN "{s}" AND "{e}"
            ORDER BY date ASC
        """.format(
            s = str((s - timedelta(5)).strftime("%Y-%m-%d")),
            e = str(e.strftime("%Y-%m-%d"))
        ), con)

        # If there are at least 5 records, a moving average can be calculated
        if df.shape[0] >= 5:
            # The moving average is calculated using the rolling function on pandas
            df["moving_average"] = df["price"].rolling(5).mean()

            # Null values are filtered for simplicity
            df = df[df["moving_average"].notnull()]
            
            # SQL code to update the DB, it needs to be done manually as pandas does not support updating
            sql = "INSERT OR REPLACE INTO bitcoin_price (date, price, moving_average) VALUES "
            for _, row in df.iterrows():
                sql += """("{date}", {price}, {ma}), """.format(
                    date = row["date"],
                    price = row["price"],
                    ma = row["moving_average"]
                )
            cur = con.cursor()
            cur.execute(sql[:len(sql) - 2])
            con.commit()

# Code to get the dates from the console as arguments
parser = argparse.ArgumentParser()
parser.add_argument("start_date", help="Start date dd-mm-yyyy")
parser.add_argument("end_date", help="End date dd-mm-yyyy")

config = vars(parser.parse_args())

# Date parsing
s = datetime.strptime(config["start_date"], "%d-%m-%Y")
e = datetime.strptime(config["end_date"], "%d-%m-%Y")

# Backfill execution
get_data()
calculate_moving_average()