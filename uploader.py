from supabase import create_client, Client
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from typing import Dict
from sqlalchemy import text
import os

# Load environment variables from the .env file
load_dotenv()

# Database configuration setup using environment variables
db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": os.getenv("DB_PORT")
}

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Creating the SQLAlchemy database URL
## URL encoding for username and password to handle special characters
encoded_username = quote_plus(db_config["user"])
encoded_password = quote_plus(db_config["password"])
SQLALCHEMY_DATABASE_URL = f'mysql+mysqlconnector://{encoded_username}:{encoded_password}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def db_connection():
    """
    Provides a session to interact with the database.
    Ensures that the connection is properly closed after usage.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_users_from_mysql(db: Session):
    try:
        users_query = text("""
            select user_name,user_email,user_phone from tenants where user_email != "";
            """)
        results = db.execute(users_query).fetchall()
        data = []
        for row in results:
            data.append({
                "name": row.user_name,
                "email": row.user_email,
                "phone": row.user_phone
            })
        return {"status": "success","data": data}
    except Exception as e:
        raise e

def create_user(user: Dict) -> None:
    try:
        user_response = supabase.auth.sign_up({
            "email": user['email'],
            "password": 'Changepas@123',
            "email_confirm": True,
            "phone": user['phone'],
            "user_metadata": {
                "name": user['name'],
                "phone_number": user['phone'],
                "user_type": 'tenant',
                "role": 'admin'
            }
        })
        if user_response.user and user_response.user.id:
            print(f"User created successfully: {user['email']}")
        else:
            raise Exception("User creation failed")
    except Exception as e:
        print(f"Error creating user {user['email']}: {str(e)}")


if __name__ == "__main__":
    db = next(db_connection())
    users = get_users_from_mysql(db)
    for user in users['data']:
        create_user(user)