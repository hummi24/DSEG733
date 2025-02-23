import psycopg2
import random
import datetime
from faker import Faker

# Initialize Faker to generate random names, addresses, etc.
fake = Faker()

# Connect to PostgreSQL Database
def connect_db():
    return psycopg2.connect(
        dbname="dseg733",
        user="humaira",
        password="chilleasy",
        host="postgres",
        port="5432"
    )

# Function to generate random dates within a range
def random_date(start_year=2014, end_year=2024):
    start_date = datetime.date(start_year, 1, 1)
    end_date = datetime.date(end_year, 12, 31)
    return start_date + datetime.timedelta(days=random.randint(0, (end_date - start_date).days))

# Function to generate synthetic data and populate tables
def populate_database():
    conn = connect_db()
    cur = conn.cursor()
    
    # Insert data into Dim_Customer
    for _ in range(10000):
        cur.execute("""
            INSERT INTO midterm.Dim_Customer (Name, DOB, Address, Gender, Age_Group)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            fake.name(),
            fake.date_of_birth(minimum_age=18, maximum_age=90),
            fake.address(),
            random.choice(["Male", "Female", "Other"]),
            random.choice(["18-25", "26-35", "36-45", "46-60", "60+"])
        ))
    
    # Insert data into Dim_Cinema
    for _ in range(500):
        cur.execute("""
            INSERT INTO midterm.Dim_Cinema (Name, Address)
            VALUES (%s, %s)
        """, (
            fake.company(),
            fake.address()
        ))
    
    # Insert data into Dim_Hall
    for _ in range(1000):
        cur.execute("""
            INSERT INTO midterm.Dim_Hall (Size, Cinema_ID)
            VALUES (%s, %s)
        """, (
            random.randint(50, 500),
            random.randint(1, 500)  # Randomly link to existing cinemas
        ))
    
    # Insert data into Dim_Movie
    for _ in range(1000):
        cur.execute("""
            INSERT INTO midterm.Dim_Movie (Title, Genre, ReleaseDate, Language, Cost, Country, Director_Name, Star_Name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            fake.catch_phrase(),
            random.choice(["Action", "Comedy", "Drama", "Horror", "Sci-Fi"]),
            random_date(2000, 2024),
            random.choice(["English", "French", "Spanish", "German"]),
            round(random.uniform(1000000, 100000000), 2),
            fake.country(),
            fake.name(),
            fake.name()
        ))
    
    # Insert data into Dim_Showing
    for _ in range(2000):
        cur.execute("""
            INSERT INTO midterm.Dim_Showing (Movie_ID, Date, Time, Hall_ID)
            VALUES (%s, %s, %s, %s)
        """, (
            random.randint(1, 1000),
            random_date(2014, 2024),
            fake.time(),
            random.randint(1, 1000)
        ))
    
    # Insert data into Fact_TicketSales
    for _ in range(1000000):  # Generating 1M records
        cur.execute("""
            INSERT INTO midterm.Fact_TicketSales (
                Transaction_ID, Customer_ID, Movie_ID, Showing_ID, Hall_ID, Cinema_ID, Promotion_ID, 
                Payment_Method, Transaction_Type, Total_Price, Transaction_Date, Transaction_Time,
                Transaction_Year, Transaction_Month, Weekday_Weekend, Tickets_Count, Time_of_Day
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            _,  # Unique transaction ID
            random.randint(1, 10000),  # Link to customer
            random.randint(1, 1000),  # Link to movie
            random.randint(1, 2000),  # Link to showing
            random.randint(1, 1000),  # Link to hall
            random.randint(1, 500),  # Link to cinema
            random.randint(1, 100),  # Link to promotion
            random.choice(["Credit Card", "Cash", "PayPal"]),
            random.choice(["Online", "Offline"]),
            round(random.uniform(5, 100), 2),
            random_date(2014, 2024),
            fake.time(),
            random.randint(2014, 2024),
            random.randint(1, 12),
            random.choice(["Weekday", "Weekend"]),
            random.randint(1, 10),
            random.choice(["Morning", "Afternoon", "Evening", "Night"])
        ))
    
    # Commit all inserts and close connection
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Database populated successfully with synthetic data!")

if __name__ == "__main__":
    populate_database()
