import requests
from prefect import task, Flow
from datetime import datetime, timedelta
import sqlite3
import pandas as pd
from prefect.schedules import IntervalSchedule

# The ID needed by the API
CRYPTO_ID = "bitcoin"

# The in-memory database file
DB_NAME = "bitcoin.db"

# Task that downloads the price of an specified date and returns it as a float
@task
def download_price(date: datetime) -> float:
    url = """https://api.coingecko.com/api/v3/coins/{ID}/history?date={DATE}""".format(
        ID = CRYPTO_ID,
        DATE = date.strftime("%d-%m-%Y")
    )
    res = requests.get(url).json()
    return res["market_data"]["current_price"]["usd"]

# Function to create the table on the DB if it doesn't exist yet
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

# Task to add the price value of the specified date to the database
@task
def save_data(date: datetime, price: float):
    create_db()
    with sqlite3.connect(DB_NAME) as con:
        cur = con.cursor()
        sql = """
            INSERT OR REPLACE INTO bitcoin_price (date, price, moving_average)
            VALUES ("{DATE}", {PRICE}, NULL)
        """.format(
            DATE = str(date.strftime("%Y-%m-%d")),
            PRICE = price
        )
        cur.execute(sql)
        con.commit()

# Task that calculates the moving average and updates the DB
@task
def calculate_moving_average(date: datetime):
    with sqlite3.connect(DB_NAME) as con:
        # Read the last 5 entries on the DB by date with respect to the given date
        df = pd.read_sql("""
            SELECT 
                *
            FROM bitcoin_price
            WHERE date <= "{DATE}"
            ORDER BY date DESC
            LIMIT 5
        """.format(
            DATE = str(date.strftime("%Y-%m-%d"))
        ), con)

        # If there are at least 5 records we can proceed, otherwise we exit the function
        if df.shape[0] >= 5:

            # Calculate the window average using the rolling function of pandas
            df.at[0, "moving_average"] = df["price"].rolling(5).mean().iloc[4]

            # Filter null values for eficiency
            df = df[df["moving_average"].notnull()]

            # Build the SQL to update the DB
            #
            # Pandas do not support updating records on the DB, so this had to be
            # executed manually, otherwise there would be an error if processing a day that
            # already exists.
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
    return True

# Scheduler to run the pipeline daily
scheduler = IntervalSchedule(
    interval=timedelta(days=1)
)

# Main pipeline that execute functions in the proper order
def get_data():
    with Flow("get_data", schedule=scheduler) as flow:
        # It takes the current day at runtime
        date = datetime.now()
        price = download_price(date)
        saved = save_data(date, price, upstream_tasks=[price]) 
        calculate_moving_average(date, upstream_tasks=[saved])
    return flow

if __name__ == '__main__':
    flow = get_data()
    flow.run()