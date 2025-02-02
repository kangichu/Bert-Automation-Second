import os
import sys
import random
from openai import OpenAI, RateLimitError, APIError
import asyncio
from datetime import datetime, timedelta
from transformers import GPT2LMHeadModel, GPT2Tokenizer
import mysql.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set the environment variable to disable oneDNN custom operations
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

# Initialize OpenAI client
try:
    client = OpenAI(api_key=os.getenv('OPEN_AI_GPT_ACCESS_TOKEN', ''))
    print(f"OpenAI API key loaded: {os.getenv('OPEN_AI_GPT_ACCESS_TOKEN')}")
except Exception as e:
    print(f"Error initializing OpenAI client: {e}")
    sys.exit(1)

# MySQL Configuration
MYSQL_CONFIG = {
    'host': os.getenv('SQL_HOST', 'localhost'),
    'user': os.getenv('SQL_USERNAME', 'root'),
    'password': os.getenv('SQL_PASSWORD', ''),
    'database': os.getenv('SQL_DATABASE', 'test'),
    'port': int(os.getenv('SQL_PORT', 3306))
}

# Define price ranges based on category
price_ranges = {
    "Sale": {
        "min": 5000000,  # 5 million
        "max": 50000000  # 50 million
    },
    "Rent": {
        "min": 50000,    # 50,000
        "max": 500000    # 500,000
    }
}

# Mapping property types to their respective names
property_name_map = {
    "Apartment": [
        "Skyline Heights", "Ocean Breeze Apartments", "Emerald Residences", "Crystal Towers", "The Grandview Apartments",
        "Cityscape Lofts", "Azure Heights", "The Horizon Residences", "Sunset Towers", "The Metropolitan Suites",
        "Urban Sky Apartments", "Starlight Residences", "Lakeside Heights", "Celestial Towers", "The Pinnacle Apartments"
    ],
    "Villa": [
        "Palmview Villas", "Serenity Villas", "Golden Palms Estate", "Sunset Haven Villas", "Azure Bay Villas",
        "Majestic Palm Villas", "Tranquil Shores Villas", "Orchid Blossom Villas", "The Oasis Retreat", "Royal Palm Villas",
        "Horizon Breeze Villas", "The Grand Oasis", "Seabreeze Villas", "Mountain View Villas", "Whispering Palms Retreat"
    ],
    "Townhouse": [
        "Maplewood Townhouses", "The Greenhaven Towns", "Oakridge Townhomes", "Cedar Lane Residences", "Riverside Townhouses",
        "Willow Creek Townhomes", "Amber Ridge Townhouses", "Brookside Residences", "The Haven Towns", "Woodland Townhomes",
        "Sunset Park Townhouses", "Spring Blossom Townhomes", "Lush Garden Towns", "The Courtyard Townhouses", "Horizon View Townhomes"
    ],
    "Penthouse": [
        "Skyline Penthouse", "Celestial Heights", "The Horizon Penthouse", "Eclipse Towers", "Infinity Sky Suites",
        "Luxe Panorama Penthouse", "The Summit Residence", "Grand Skylight Penthouse", "The Stellar Suite", "Cloud Nine Penthouse",
        "Zenith View Penthouse", "The Skyline Palace", "The Aether Suite", "Celestial Peak Penthouse", "The Imperial Penthouse"
    ],
    "Studio": [
        "Metro Studio Suites", "Uptown Lofts", "Urban Nest Studios", "Cozy Corner Studios", "The Minimalist Loft",
        "Skyview Studios", "The Modernist Loft", "The Chic Haven", "Infinity Studio Apartments", "Crystal Loft Studios",
        "The Compact Living Studios", "Urban Skyline Studios", "Modish Micro-Lofts", "Luxe Living Studios", "Zen Space Studios"
    ],
    "House": [
        "The Alfem House", "Golden Crest Manor", "Silver Sands House", "Willow Creek Home", "Horizon Estates",
        "The Grand Manor", "Evergreen Estate", "Sunrise Meadows Home", "Tranquility Haven", "The Homestead",
        "Lakeview Cottage", "Maple Grove House", "Aspen Heights Residence", "The Majestic Home", "Starlight Manor"
    ],
    "New Development": [
        "The Pearl Residences", "Grand Horizon Development", "The Emerald City Project", "Urban Oasis", "Pioneer Heights",
        "The Sapphire Complex", "Skyline Square", "Golden Future Residences", "Serene Urban Developments", "Majestic Heights",
        "The Vanguard Residences", "Metropolitan Grand", "Infinity Horizons", "The Zenith Project", "Celestial Plaza"
    ],
    "Cottage": [
        "Whispering Pines Cottage", "Tranquil Haven", "Bluebird Cottage", "Meadowview Lodge", "Hidden Valley Cottages",
        "Lakeside Retreat", "The Cozy Hearth", "Sunflower Cottage", "The Willow Retreat", "Rustic Charm Cottage",
        "Horizon Bloom Cottage", "Forest Haven Cottages", "The Serene Grove", "Sunset Meadow Cottage", "Riverside Hideaway"
    ],
    "Duplex": [
        "Twin Peaks Duplex", "Sunrise Twin Homes", "Symmetry Residences", "The Dual Haven", "Evergreen Duplexes",
        "Mirrorview Duplex", "Horizon Twin Residences", "Parallel Suites", "The Urban Twin Villas", "Cedar Park Duplex",
        "Dual Serenity Homes", "Golden Symmetry Duplex", "The Perfect Pair Duplex", "Stellar Twin Residences", "Oasis Twin Homes"
    ]
}

# Define square area ranges based on type
sq_area_ranges = {
    "Studio": {
        "min": 300,
        "max": 600
    },
    "Apartment": {
        "min": 600,
        "max": 1200
    },
    "Penthouse": {
        "min": 1500,
        "max": 3000
    },
    "Villa": {
        "min": 2000,
        "max": 5000
    },
    "House": {
        "min": 1500,
        "max": 5000
    },
    "Townhouse": {
        "min": 1200,
        "max": 2500
    },
    "New Development": {
        "min": 1000,
        "max": 10000
    },
    "Cottage": {
        "min": 800,
        "max": 2000
    },
    "Duplex": {
        "min": 1200,
        "max": 3000
    }
}

# Load pre-trained GPT-2 model and tokenizer
try:
    model_name = "gpt2"  # Use "gpt2-medium" or "gpt2-large" for better results
    tokenizer = GPT2Tokenizer.from_pretrained(model_name)
    model = GPT2LMHeadModel.from_pretrained(model_name)
    tokenizer.pad_token = tokenizer.eos_token  # Set pad_token_id to eos_token_id to avoid warnings
except Exception as e:
    print(f"Error loading GPT-2 model: {e}")
    sys.exit(1)

# Function to generate text using GPT-2
def generate_text_gpt2(prompt, max_length=50, num_return_sequences=1):
    try:
        inputs = tokenizer.encode(prompt, return_tensors="pt", max_length=50, truncation=True)
        attention_mask = inputs.ne(tokenizer.pad_token_id).float()  # Create attention mask
        outputs = model.generate(
            inputs,
            attention_mask=attention_mask,  # Pass attention mask
            max_length=max_length,
            num_return_sequences=num_return_sequences,
            no_repeat_ngram_size=2,
            top_k=50,
            top_p=0.95,
            temperature=0.7,
            do_sample=True,
            pad_token_id=tokenizer.eos_token_id  # Explicitly set pad_token_id
        )
        results = [tokenizer.decode(output, skip_special_tokens=True) for output in outputs]
        return results
    except Exception as e:
        print(f"Error generating text with GPT-2: {e}")
        return ["Error generating text."]

# Function to generate text using OpenAI's GPT-3.5
async def generate_text(prompt, max_tokens=100, temperature=0.7):
    try:
        completion = await client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return completion.choices[0].message.content.strip()
    except RateLimitError:
        print("OpenAI API quota exceeded. Falling back to GPT-2 or placeholder text.")
        return None
    except APIError as e:
        print(f"OpenAI API error: {e}")
        return None
    except Exception as e:
        print(f"Error generating text with OpenAI: {e}")
        return None

# Function to generate a random date within a range
def random_date(start, end):
    try:
        delta = end - start
        random_days = random.randint(0, delta.days)
        return start + timedelta(days=random_days)
    except Exception as e:
        print(f"Error generating random date: {e}")
        return datetime.now()

# Function to create a database connection
def create_mysql_connection():
    try:
        print("Connecting to MySQL database...")
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        print("Connected to MySQL database successfully!")
        return conn
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL database: {e}")
        sys.exit(1)

# Function to create the listings table if it doesn't exist
def create_table(conn):
    try:
        print("Creating table 'listings_datasets' if it doesn't exist...")
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS listings_datasets (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name TEXT,
                ref TEXT,
                slug TEXT,
                category TEXT,
                county TEXT,
                county_specific TEXT,
                longitude DECIMAL(9,6),
                latitude DECIMAL(9,6),
                location_description TEXT,
                type TEXT,
                class TEXT,
                furnishing TEXT,
                bedrooms INT,
                bathrooms INT,
                sq_area INT,
                amount INT,
                viewing_fee INT,
                property_description TEXT,
                status TEXT,
                availability TEXT,
                subscription_status TEXT,
                complex_id INT,
                user_id INT,
                created_at DATETIME,
                updated_at DATETIME,
                link TEXT,
                currency TEXT
            )
        ''')
        conn.commit()
        print("Table 'listings_datasets' created or already exists.")
    except mysql.connector.Error as e:
        print(f"Error creating table: {e}")
        sys.exit(1)

# Function to insert a listing into the database
def insert_listing(conn, listing):
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO listings_datasets (
                name, ref, slug, category, county, county_specific, longitude, latitude,
                location_description, type, class, furnishing, bedrooms, bathrooms, sq_area,
                amount, viewing_fee, property_description, status, availability,
                subscription_status, complex_id, user_id, created_at, updated_at, link, currency
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', listing)
        conn.commit()
    except mysql.connector.Error as e:
        print(f"Error inserting listing into database: {e}")
        conn.rollback()

# Function to generate a random amount based on category
def generate_amount(category):
    try:
        if category not in price_ranges:
            raise ValueError(f"Invalid category: {category}")
        return random.randint(price_ranges[category]["min"], price_ranges[category]["max"])
    except Exception as e:
        print(f"Error generating amount: {e}")
        return 0

# Function to generate a random square area based on type
def generate_sq_area(property_type):
    try:
        if property_type not in sq_area_ranges:
            raise ValueError(f"Invalid property type: {property_type}")
        return random.randint(sq_area_ranges[property_type]["min"], sq_area_ranges[property_type]["max"])
    except Exception as e:
        print(f"Error generating square area: {e}")
        return 0

# Function to generate a random name based on type
def generate_name(property_type):
    try:
        return random.choice(property_name_map[property_type])
    except Exception as e:
        print(f"Error generating property name: {e}")
        return "Unknown Property"

# Generate a full dataset
async def generate_dataset(num_listings=5):
    print(f"Starting dataset generation for {num_listings} listings...")
    start_time = datetime.now()
    print(f"Start time: {start_time}")

    # Sample data for generating realistic listings
    categories = ["Sale", "Rent"]
    counties = ["Nairobi County", "Mombasa County", "Kisumu County", "Nakuru County"]
    divisions = ["Kilimani", "Westlands", "Kileleshwa", "Karen", "Langata", "Kawangware"]
    types = ["Apartment", "Villa", "Townhouse", "Penthouse", "Studio", "House", "New Development", "Cottage", "Duplex"]
    classes = ["Luxury", "Regular", "Affordable"]
    furnishings = ["Furnished", "Unfurnished", "Partially Furnished"]
    bedrooms = [1, 2, 3, 4, 5]
    bathrooms = [1, 2, 3, 4]
    viewing_fees = [0, 1000, 2000, 3000, 5000]
    statuses = ["Published", "Draft"]
    availabilities = ["Available", "Unavailable"]
    complex_ids = [33, 34, None]
    user_ids = [4, 5, 6, 7]
    currencies = ["KES", "USD"]

    # Create MySQL connection
    conn = create_mysql_connection()
    create_table(conn)

    # Generate listings
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)

    for i in range(1, num_listings + 1):
        # Generate title
        title = generate_name(random.choice(types))

        # Generate location description
        location_prompt = "Write a short and engaging description for the location of a property in " + random.choice(divisions) + ": "
        location_description = await generate_text(location_prompt, max_tokens=50)
        if location_description is None:
            # Fallback to GPT-2 or placeholder text
            location_description = generate_text_gpt2(location_prompt, max_length=50, num_return_sequences=1)[0]

        # Generate property description
        property_prompt = "Describe a " + random.choice(classes) + " " + random.choice(types) + " in " + random.choice(counties) + ": "
        property_description = await generate_text(property_prompt, max_tokens=100)
        if property_description is None:
            # Fallback to GPT-2 or placeholder text
            property_description = generate_text_gpt2(property_prompt, max_length=100, num_return_sequences=1)[0]

        # Generate other fields
        ref = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=10))
        slug = title.lower().replace(" ", "-").replace(",", "").replace(".", "")
        category = random.choice(categories)
        county = random.choice(counties)
        county_specific = random.choice(divisions)
        longitude = round(random.uniform(36.70, 36.90), 6)
        latitude = round(random.uniform(-1.40, -1.20), 6)
        type_ = random.choice(types)
        class_ = random.choice(classes)
        furnishing = random.choice(furnishings) if type_ != "Land" else None
        bedroom = random.choice(bedrooms) if type_ != "Land" else None
        bathroom = random.choice(bathrooms) if type_ != "Land" else None
        sq_area = generate_sq_area(type_)
        amount = generate_amount(category)  # Generate amount based on category
        viewing_fee = random.choice(viewing_fees)
        status = random.choice(statuses)
        availability = random.choice(availabilities)
        subscription_status = None
        complex_id = random.choice(complex_ids)
        user_id = random.choice(user_ids)
        created_at = random_date(start_date, end_date).strftime("%Y-%m-%d %H:%M:%S")
        updated_at = (datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S") + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S")
        link = None
        currency = random.choice(currencies)

        # Create listing tuple
        listing = (
            title, ref, slug, category, county, county_specific, longitude, latitude,
            location_description, type_, class_, furnishing, bedroom, bathroom, sq_area,
            amount, viewing_fee, property_description, status, availability, subscription_status,
            complex_id, user_id, created_at, updated_at, link, currency
        )

        # Insert into database
        insert_listing(conn, listing)

        if i % 100 == 0:
            print(f"Generated {i} listings...")

    end_time = datetime.now()
    print(f"Finished generating {num_listings} listings.")
    print(f"End time: {end_time}")
    print(f"Total time taken: {end_time - start_time}")
    conn.close()


if __name__ == "__main__":
    try:
        # Run the script asynchronously
        asyncio.run(generate_dataset(num_listings=5))
    except KeyboardInterrupt:
        print("\nProcess interrupted by user (Ctrl+C). Cleaning up and exiting...")
        sys.exit(0)  # Exit gracefully