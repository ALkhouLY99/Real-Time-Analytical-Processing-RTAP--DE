import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file (if using python-dotenv)
load_dotenv()






if __name__ == "__main__":
    try:
        conn=psycopg2.connect(host= os.getenv("DB_HOST"),dbname= os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
        )
        
        cur = conn.cursor()
        ff= cur.execute("""
        
        select * from voters
        """)
        print(ff)
        
        print("Connected to the database successfully!")
    except Exception as e:
        print(f"Error connecting to the database: {e}")