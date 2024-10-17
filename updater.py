from supabase import create_client, Client
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from sqlalchemy import text
import os


load_dotenv()

db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": os.getenv("DB_PORT")
}

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

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
            select distinct distinct user_email,user_phone from tenants where user_email != "" and user_phone != "";
            """)
        results = db.execute(users_query).fetchall()
        data = []
        for row in results:
            data.append({
                "email": row.user_email,
                "phone_number": row.user_phone
            })
        return {"status": "success","data": data}
    except Exception as e:
        raise e

def update_supabase_user(email, phone_number):
    user = supabase.table('users').select("*").eq('email', email).execute()

    if user.data:
        supabase.table('users').update({'phone_number': phone_number}).eq('email', email).execute()
        print(f"Updated phone for user: {email}")
    else:
        print(f"User with email {email} not found in Supabase.")

def sync_data():
    db = next(db_connection())
    response = get_users_from_mysql(db)
    mysql_users = response['data']
    for user in mysql_users:
        email = user['email']
        phone_number = user['phone_number']
        update_supabase_user(email, phone_number)

if __name__ == "__main__":
    sync_data()
