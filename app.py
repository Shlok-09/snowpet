# streamlit_app.py

import streamlit as st
import snowflake.connector
import pandas as pd

import pandas_datareader as web
# import pandas as pd
import datetime as dt
import yfinance as yf
 
start = dt.datetime(2010,1,1)
end=dt.datetime(2020,1,1)

yf.pdr_override()

df_ent = yf.download('ADANIENT.NS', start, end)
# df_ports = yf.download('ADANIPORTS.NS', start, end)
# df_power = yf.download("ADANIPOWER.NS", start, end)
# df_green = yf.download("ADANIGREEN.NS", start, end)
# df_tran = yf.download("ADANITRANS.NS", start, end)
# df_gas = yf.download("ATGL.NS", start, end)

df_ent.rename(columns = {'Adj Close':'Adj_Close'}, inplace = True)

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

conn = init_connection()

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

# rows = run_query("SELECT * from mytable;")
cur = conn.cursor()

for index, row in df_ent.iterrows():
     cur.execute("INSERT INTO ADANIENT (Date,Open,High,Low,Close,Adj_Close,Volume) values(?,?,?,?,?,?,?)", row.Date, row.Open, row.High, row.Low, row.Close, row.Adj_close, row.Volume)
conn.commit()
cur.close()

# Print results.
# for row in rows:
#     st.write(f"{row[0]} has a :{row[1]}:")