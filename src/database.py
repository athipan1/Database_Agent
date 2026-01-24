import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables from .env file for local development
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

try:
    # Create the SQLAlchemy engine
    engine = create_engine(DATABASE_URL, echo=False) # Set echo=True for debugging SQL queries

    # Create a configured "Session" class
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

except Exception as e:
    print(f"Failed to connect to the database. Please check your DATABASE_URL.")
    print(f"Error: {e}")
    # Exit or raise a custom exception to prevent the application from running without a database
    raise

def get_db_session():
    """
    Dependency function to get a new database session.
    Ensures the session is always closed after use.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_tables():
    """
    Create all tables in the database that are defined in the Base metadata.
    This is typically called once at application startup.
    """
    from .models import Base
    print("Creating database tables if they don't exist...")
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully.")

if __name__ == '__main__':
    # This block allows running `python -m src.database` to create tables manually.
    print("Running database setup...")
    create_tables()
