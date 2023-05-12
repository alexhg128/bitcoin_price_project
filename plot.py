import sqlite3
import pandas as pd
import plotly.graph_objects as go

# filenema of the in-memory database
DB_NAME = "bitcoin.db"

# Open connection to DB
with sqlite3.connect(DB_NAME) as con:

    # Read all records where we have a moving average
    df = pd.read_sql("""
            SELECT 
                *
            FROM bitcoin_price
            WHERE moving_average IS NOT NULL
            ORDER BY date ASC
    """, con)

    fig = go.Figure()

    # Add the price line to the plot
    fig.add_trace(
        go.Line(
            x=df["date"], y=df["price"],
            name="Price", yaxis='y'
        )
    )

    # Add the moving average line to the plot
    fig.add_trace(
        go.Line(
            x=df["date"], y=df["moving_average"],
            name="Moving Average", yaxis='y'
        )
    )

    fig.update_layout(title_text="Bitcoin Historic Price")

    fig.show()