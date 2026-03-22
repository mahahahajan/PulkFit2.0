# scripts/db_utils.py
import sqlite3
import pandas as pd
import os

# 1. Get the absolute path of the directory containing this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Define the path to the data folder (inside PulkFit2.0)
DATA_DIR = os.path.join(BASE_DIR, 'data')

# 3. Define the exact path for the database file
DB_PATH = os.path.join(DATA_DIR, 'pulkfit.db')

def get_connection():
    return sqlite3.connect(DB_PATH)

def log_daily_metrics(date_str, weight, sleep, mobility, calories, protein):
    """Inserts or updates a daily log entry."""
    conn = get_connection()
    cursor = conn.cursor()
    
    query = '''
        INSERT INTO morning_metrics (date, weight_lbs, sleep_hours, mobility_done, calories, protein_g)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            weight_lbs=excluded.weight_lbs,
            sleep_hours=excluded.sleep_hours,
            mobility_done=excluded.mobility_done,
            calories=excluded.calories,
            protein_g=excluded.protein_g
    '''
    cursor.execute(query, (date_str, weight, sleep, mobility, calories, protein))
    conn.commit()
    conn.close()

def get_recent_metrics(days=14):
    """Fetches recent data and calculates the 7-day rolling averages."""
    conn = get_connection()
    # Read directly into a Pandas DataFrame for easy time-series math
    df = pd.read_sql("SELECT * FROM morning_metrics ORDER BY date ASC", conn)
    conn.close()
    
    if df.empty:
        return df
        
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    # Calculate 7-day rolling averages
    df['weight_7d_avg'] = df['weight_lbs'].rolling(window=7, min_periods=1).mean()
    df['sleep_7d_avg'] = df['sleep_hours'].rolling(window=7, min_periods=1).mean()
    
    # Return the tail end of the dataframe
    return df.tail(days)