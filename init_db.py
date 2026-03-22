import sqlite3
import os

# 1. Get the absolute path of the directory containing this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Define the path to the data folder (inside PulkFit2.0)
DATA_DIR = os.path.join(BASE_DIR, 'data')

# 3. Define the exact path for the database file
DB_PATH = os.path.join(DATA_DIR, 'pulkfit.db')

def initialize_database():
    # THE FIX: Force Python to create the 'data' directory if it's missing
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Now SQLite is guaranteed to find the directory and will create the file
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Morning Metrics Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS morning_metrics (
            date DATE PRIMARY KEY,
            weight_lbs REAL,
            sleep_hours REAL,
            mobility_done BOOLEAN,
            calories INTEGER,
            protein_g INTEGER
        )
    ''')
    
    # Lifting Log Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lifting_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE,
            movement TEXT,
            weight_lbs REAL,
            sets INTEGER,
            reps INTEGER,
            rpe REAL,
            notes TEXT
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"Database initialized successfully at: {DB_PATH}")

if __name__ == "__main__":
    initialize_database()