import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta

# Database connection details
DB_HOST = "localhost"
DB_NAME = "dseg733"
DB_USER = "humaira"
DB_PASS = "chilleasy"
DB_PORT = "5432"

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    port=DB_PORT
)
cur = conn.cursor()

# Create Faker instance
fake = Faker()

# Create empty lists to store IDs for relationships
customer_ids = []
movie_ids = []
cinema_ids = []
hall_ids = []
showing_ids = []
promotion_ids = []

# Populate dim_cinema (20 cinemas)
print("Generating cinema data...")
for _ in range(20):
    name = fake.company() + " Cinema"
    address = fake.address().replace('\n', ', ')
    
    cur.execute("""
        INSERT INTO dim_cinema (name, address)
        VALUES (%s, %s) RETURNING cinema_id;
    """, (name, address))
    
    cinema_id = cur.fetchone()[0]
    cinema_ids.append(cinema_id)

# Populate dim_hall (3-5 halls per cinema)
print("Generating hall data...")
for cinema_id in cinema_ids:
    for _ in range(random.randint(3, 5)):
        size = random.randint(50, 300)
        
        cur.execute("""
            INSERT INTO dim_hall (size, cinema_id)
            VALUES (%s, %s) RETURNING hall_id;
        """, (size, cinema_id))
        
        hall_id = cur.fetchone()[0]
        hall_ids.append(hall_id)

# Populate dim_movie (100 movies)
print("Generating movie data...")
languages = ['English', 'Spanish', 'French', 'German', 'Chinese', 'Japanese', 'Korean', 'Hindi']
countries = ['USA', 'UK', 'France', 'Germany', 'Spain', 'Japan', 'South Korea', 'India', 'China', 'Australia']
for _ in range(100):
    title = fake.catch_phrase()
    genre = random.choice(['Action', 'Comedy', 'Drama', 'Thriller', 'Romance', 'Horror', 'Sci-Fi', 'Adventure', 'Animation', 'Documentary'])
    releasedate = fake.date_between(start_date='-5y', end_date='today')
    language = random.choice(languages)
    cost = round(random.uniform(100000, 9999999), 2)  # Maximum of 7 digits before decimal to fit numeric(10,2)
    country = random.choice(countries)
    star_name = fake.name()
    director_name = fake.name()
    
    cur.execute("""
        INSERT INTO dim_movie (title, genre, releasedate, language, cost, country, star_name, director_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING movie_id;
    """, (title, genre, releasedate, language, cost, country, star_name, director_name))
    
    movie_id = cur.fetchone()[0]
    movie_ids.append(movie_id)

# Populate dim_customer (1000 customers)
print("Generating customer data...")
for _ in range(1000):
    name = fake.name()
    dob = fake.date_of_birth(minimum_age=18, maximum_age=90)
    gender = random.choice(['Male', 'Female', 'Other'])
    address = fake.address().replace('\n', ', ')
    
    cur.execute("""
        INSERT INTO dim_customer (name, dob, gender, address)
        VALUES (%s, %s, %s, %s) RETURNING customer_id;
    """, (name, dob, gender, address))
    
    customer_id = cur.fetchone()[0]
    customer_ids.append(customer_id)

# Populate dim_promotion (20 promotions)
print("Generating promotion data...")
for _ in range(20):
    description = fake.paragraph(nb_sentences=2)
    discount = round(random.uniform(0.05, 0.5), 2)  # 5% to 50% discount
    startdate = fake.date_between(start_date='-1y', end_date='today')
    enddate = fake.date_between(start_date=startdate, end_date=startdate + timedelta(days=90))
    
    cur.execute("""
        INSERT INTO dim_promotion (description, discount, startdate, enddate)
        VALUES (%s, %s, %s, %s) RETURNING promotion_id;
    """, (description, discount, startdate, enddate))
    
    promotion_id = cur.fetchone()[0]
    promotion_ids.append(promotion_id)

# Populate dim_showing (showings for movies in halls)
print("Generating showing data...")
for _ in range(500):
    movie_id = random.choice(movie_ids)
    date = fake.date_between(start_date='-6m', end_date='+1m')
    time = fake.time_object()
    hall_id = random.choice(hall_ids)
    
    cur.execute("""
        INSERT INTO dim_showing (movie_id, date, time, hall_id)
        VALUES (%s, %s, %s, %s) RETURNING showing_id;
    """, (movie_id, date, time, hall_id))
    
    showing_id = cur.fetchone()[0]
    showing_ids.append(showing_id)

# Populate fact_ticketsales (2000 transactions)
print("Generating ticket sales data...")
payment_methods = ['Credit Card', 'Debit Card', 'Cash', 'Mobile Payment', 'Gift Card']
transaction_types = ['Online', 'Kiosk', 'Counter', 'Mobile App']
time_of_day = ['Morning', 'Afternoon', 'Evening', 'Night']
browsers = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Mobile Safari', 'Mobile Chrome', 'App']

for i in range(2000):
    # Basic data
    transaction_id = i + 1000  # Start from 1000
    customer_id = random.choice(customer_ids)
    movie_id = random.choice(movie_ids)
    showing_id = random.choice(showing_ids)
    
    # Get hall_id and cinema_id for the showing
    cur.execute("SELECT hall_id FROM dim_showing WHERE showing_id = %s", (showing_id,))
    hall_id = cur.fetchone()[0]
    
    cur.execute("SELECT cinema_id FROM dim_hall WHERE hall_id = %s", (hall_id,))
    cinema_id = cur.fetchone()[0]
    
    # Maybe use a promotion
    promotion_id = random.choice([None] + promotion_ids) if random.random() < 0.3 else None
    
    # Transaction details
    payment_method = random.choice(payment_methods)
    transaction_type = random.choice(transaction_types)
    tickets_count = random.randint(1, 6)
    base_price = random.uniform(8.0, 15.0)
    
    # Calculate total price
    total_price = round(tickets_count * base_price, 2)
    
    # Apply promotion discount if any
    if promotion_id:
        cur.execute("SELECT discount FROM dim_promotion WHERE promotion_id = %s", (promotion_id,))
        discount = cur.fetchone()[0]
        # Convert Decimal to float to avoid type error
        discount_float = float(discount)
        total_price = round(total_price * (1 - discount_float), 2)
    
    # Transaction date and time
    transaction_date = fake.date_between(start_date='-6m', end_date='today')
    transaction_time = fake.time_object()
    
    # Derived time fields
    transaction_year = transaction_date.year
    transaction_month = transaction_date.month
    transaction_week = transaction_date.isocalendar()[1]
    weekday_weekend = 'Weekend' if transaction_date.weekday() >= 5 else 'Weekday'
    time_of_day_val = random.choice(time_of_day)
    
    # Browser information for online transactions
    browser = random.choice(browsers) if transaction_type in ['Online', 'Mobile App'] else None
    
    # Insert the transaction
    cur.execute("""
        INSERT INTO fact_ticketsales (
            transaction_id, customer_id, movie_id, showing_id, hall_id, cinema_id,
            promotion_id, payment_method, transaction_type, total_price,
            transaction_date, transaction_time, transaction_year, transaction_month,
            weekday_weekend, tickets_count, time_of_day, browser, transaction_week
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """, (
        transaction_id, customer_id, movie_id, showing_id, hall_id, cinema_id,
        promotion_id, payment_method, transaction_type, total_price,
        transaction_date, transaction_time, transaction_year, transaction_month,
        weekday_weekend, tickets_count, time_of_day_val, browser, transaction_week
    ))
    
    if i % 100 == 0:
        print(f"Generated {i} transactions...")

# Commit all changes
conn.commit()
cur.close()
conn.close()
print("Data generation complete!")