import os
import sys
import re
import random
from collections import defaultdict
from openai import OpenAI, RateLimitError, APIError
import asyncio
from datetime import datetime, timedelta
from transformers import GPT2LMHeadModel, GPT2Tokenizer
import mysql.connector
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
import torch
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add global counter
generation_counter = defaultdict(int)

# Initialize OpenAI client
try:
    client = OpenAI(api_key=os.getenv('OPEN_AI_GPT_ACCESS_TOKEN', ''))
    print(f"OpenAI API key loaded successfully.")
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

# Add after price_ranges dictionary
amenities = {
    'internal': [
        'modern kitchen', 'walk-in closet', 'en-suite bathroom', 'hardwood floors',
        'air conditioning', 'ceiling fans', 'built-in wardrobes', 'high ceilings',
        'open plan layout', 'marble countertops', 'modern appliances', 'breakfast bar',
        'laundry room', 'guest bathroom', 'study area', 'storage room',
        'hot water system', 'internet connectivity', 'DSTV connection', 'security alarm',
        'smart home system', 'fireplace', 'soundproof walls', 'central heating',
        'wine cellar', 'home theater', 'sauna', 'jacuzzi', 'steam room', 'elevator',
        'panic room', 'underfloor heating', 'skylights', 'bay windows', 'library',
        'gaming room', 'art studio', 'walk-in pantry', 'pet-friendly features',
        'energy-efficient lighting', 'solar water heating', 'home office', 
        'wet bar', 'mud room', 'butler pantry', 'home gym', 'meditation room',
        'music room', 'craft room', 'home spa', 'hidden room', 'biometric locks',
        'air purification system', 'radiant floor cooling', 'infrared heating',
        'greenhouse window', 'indoor pool', 'indoor garden', 'sound system',
        'home automation', 'touchless faucets', 'smart appliances', 'saltwater aquarium',
        'art gallery space', 'virtual reality room', 'wine tasting room', 'chef\'s kitchen',
        'personal cinema', 'tasting room', 'billiards room', 'darkroom for photography',
        'recording studio', 'sci-fi themed room', 'hobby room', 'escape room',
        'trophy room', 'servant quarters', 'indoor playground', 'climate-controlled wine storage',
        'book nook', 'secret passage', 'indoor waterfall', 'energy recovery ventilation',
        'geothermal heating', 'custom murals', 'star-gazing room', 'glass floor panels',
        'infinity mirror room', 'bulletproof windows', 'acoustic panels', '3D printing lab',
        'robot butler charging station', 'augmented reality room', 'bunk room', 'aquarium room'
    ],
    'external': [
        'private garden', 'balcony', 'covered parking', 'swimming pool',
        'electric fence', 'security gate', 'CCTV cameras', 'guard house',
        'children\'s playground', 'BBQ area', 'outdoor seating', 'landscaped gardens',
        'private driveway', 'backup generator', 'water storage tank', 'solar panels',
        'carport', 'perimeter wall', 'garbage collection', 'outdoor lighting',
        'tennis court', 'basketball court', 'gazebo', 'kennel', 'greenhouse',
        'pond', 'fountain', 'outdoor kitchen', 'fire pit', 'rooftop terrace',
        'helipad', 'boat dock', 'horse stable', 'orchard', 'vineyard',
        'barbed wire fencing', 'motion sensor lights', 'dog run', 'vegetable garden',
        'rainwater harvesting system', 'outdoor shower', 'tree house', 'guest cottage',
        'sports field', 'skate park', 'outdoor cinema', 'beekeeping area', 'wind turbine',
        'infinity pool', 'water slide', 'mini golf', 'outdoor gym', 'yoga platform',
        'climbing wall', 'zip line', 'archery range', 'outdoor sauna', 'hot spring',
        'sculpture garden', 'outdoor art gallery', 'botanical garden', 'labyrinth',
        'observation deck', 'solar car charging', 'EV charging station', 'wildlife pond',
        'bird watching station', 'fish pond', 'outdoor chess set', 'petanque court',
        'rock garden', 'rain garden', 'bocce ball court', 'outdoor dance floor',
        'amphitheater', 'outdoor music performance area', 'natatorium', 'sensory garden',
        'kite flying area', 'astro turf for sports', 'beach volleyball court', 'skating rink',
        'glamping site', 'outdoor library', 'hammock area', 'cable car', 'funicular',
        'tree canopy walk', 'nature trail', 'kite surfing launch', 'flower maze'
    ],
    'nearby': [
        'shopping mall', 'public transport', 'schools', 'hospitals',
        'restaurants', 'supermarket', 'gym', 'park',
        'police station', 'bank', 'pharmacy', 'places of worship',
        'main road access', 'business district', 'entertainment venues', 'medical facilities',
        'petrol station', 'market', 'coffee shops', 'sports facilities',
        'airport', 'train station', 'bus terminal', 'cinema', 'theater',
        'golf course', 'hiking trails', 'beach', 'lake', 'river',
        'university', 'college', 'daycare', 'vet clinic', 'post office',
        'art gallery', 'museum', 'zoo', 'amusement park', 'cycling paths',
        'library', 'community center', 'nightlife', 'ferry service', 'marina',
        'ski resort', 'national park', 'historical sites', 'farmer\'s market',
        'concert hall', 'botanical garden', 'water park', 'surf spots',
        'ice rink', 'indoor skydiving', 'escape room center', 'comedy club',
        'virtual reality arcade', 'laser tag', 'go-kart track', 'paintball field',
        'arcade', 'aquarium', 'observation tower', 'planetarium', 'observatory',
        'karaoke bars', 'trampoline park', 'wave pool', 'rafting center',
        'hot air balloon launch', 'paragliding spot', 'skydiving school', 'vineyard tours',
        'horseback riding trails', 'bungee jumping', 'sailing school', 'kayaking launch',
        'rock climbing gym', 'extreme sports park', 'theme park', 'wildlife sanctuary',
        'botanical research center', 'sculpture park', 'street art festival', 
        'cultural heritage site', 'carnival grounds', 'flea market', 'auction house',
        'festival grounds', 'artisan workshops', 'live music venue', 'street food market',
        'jazz club', 'folk dance hall', 'open mic night spots', 'comic book store',
        'board game cafe', 'escape game venues', 'e-sports arena', 'drone racing',
        'submarine tours', 'whale watching center', 'eco-tourism spots', 'agritourism',
        'ghost town exploration', 'ghost hunting tours', 'historical reenactment sites',
        'medieval festival venue', 'renaissance fair grounds', 'steampunk event space'
    ]
}

# Load pre-trained GPT-2 model and tokenizer
try:
    gpt2_model_name = "gpt2"
    gpt2_tokenizer = GPT2Tokenizer.from_pretrained(gpt2_model_name)
    gpt2_model = GPT2LMHeadModel.from_pretrained(gpt2_model_name)
    gpt2_tokenizer.pad_token = gpt2_tokenizer.eos_token
except Exception as e:
    print(f"Error loading GPT-2 model: {e}")
    sys.exit(1)

# Load Flan-T5-small model and tokenizer
try:
    flan_model_name = "google/flan-t5-small"
    flan_tokenizer = AutoTokenizer.from_pretrained(flan_model_name)
    flan_model = AutoModelForSeq2SeqLM.from_pretrained(flan_model_name)
except Exception as e:
    print(f"Error loading Flan-T5 model: {e}")
    sys.exit(1)

# Function to generate text using Flan-T5
def generate_text_flan(prompt, max_length=50, num_return_sequences=1):
    try:
        if not isinstance(prompt, str) or not prompt.strip():
            return ["Invalid prompt"]
        
        # Update counter and print status
        generation_counter['flan'] += 1
        print(f"\rGenerating using Flan x {generation_counter['flan']}", end='')

        # Enhanced prompt engineering
        if "Describe" in prompt:
            task_prompt = (
                f"Write a detailed property description including features, amenities, and highlights for: "
                f"{prompt.strip()}. Mention the interior features, outdoor spaces, and nearby facilities."
            )
        else:
            task_prompt = f"Write a clear and natural description for: {prompt.strip()}"
        
        inputs = flan_tokenizer(
            task_prompt,
            return_tensors="pt",
            max_length=512,
            truncation=True,
            padding=True
        )
        
        outputs = flan_model.generate(
            inputs.input_ids,
            max_length=max_length,
            num_return_sequences=num_return_sequences,
            do_sample=True,
            temperature=0.8,  # Slightly increased for more variety
            top_p=0.92,
            repetition_penalty=1.3,  # Increased to reduce repetition
            length_penalty=1.2,  # Increased to favor longer outputs
            early_stopping=True,
            num_beams=4  # Added beam search for better coherence
        )
        
        results = []
        for output in outputs:
            text = flan_tokenizer.decode(output, skip_special_tokens=True)
            text = text.strip()
            # Only accept longer, more detailed descriptions
            if text and len(text) > 25:  # Increased minimum length
                # Ensure description ends with proper punctuation
                if not text.endswith(('.', '!', '?')):
                    text += '.'
                results.append(text)
                
        return results if results else ["Generation failed"]
        
    except Exception as e:
        print(f"Error generating text with Flan-T5: {e}")
        return ["Error generating text."]

# Function to generate text using GPT-2
def generate_text_gpt2(prompt, max_length=50, num_return_sequences=1):
    try:
        if not isinstance(prompt, str) or not prompt.strip():
            return ["Invalid prompt"]
        
        print("Generating using GPT-2.")
        # Update counter and print status
        generation_counter['gpt2'] += 1
        print(f"\rGenerating using GPT-2 x {generation_counter['gpt2']}", end='')


        prefix = "Generate description: "
        cleaned_prompt = prefix + prompt.strip()
        
        inputs = gpt2_tokenizer.encode(
            cleaned_prompt,
            return_tensors="pt",
            add_special_tokens=True,
            max_length=50,
            truncation=True
        )
        
        prompt_tokens = inputs[0].tolist()
        prompt_length = len(prompt_tokens)
        
        outputs = gpt2_model.generate(
            inputs,
            max_length=max_length + prompt_length,
            num_return_sequences=num_return_sequences,
            no_repeat_ngram_size=3,
            top_k=40,
            top_p=0.92,
            temperature=0.85,
            do_sample=True,
            pad_token_id=gpt2_tokenizer.eos_token_id,
            min_length=prompt_length + 10,
            repetition_penalty=1.5
        )
        
        results = []
        for output in outputs:
            new_tokens = output[prompt_length:]
            text = gpt2_tokenizer.decode(new_tokens, skip_special_tokens=True)
            text = text.strip()
            
            prompt_text = prompt.lower()
            if (text and len(text) > 10 and 
                prompt_text not in text.lower() and
                "generate description" not in text.lower()):
                results.append(text)
        
        return results if results else ["Generation failed to produce valid output"]
        
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
        print("OpenAI API quota exceeded. Falling back to Flan or GPT-2.")
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

# Function to generate a random name based on type manually
def generate_property_name_manually(property_type):
    try:
        return random.choice(property_name_map[property_type])
    except Exception as e:
        print(f"Error generating property name: {e}")
        return "Unknown Property"

# Function to generate a random property name based on type using Flan-T5
def generate_property_names(property_type):
    """Generate single property name using Flan-T5 model"""
    try:
        prompt = (
            f"Generate exactly one luxury property name in this format:\n"
            f"Format: [Adjective] [Feature] {property_type}\n"
            f"Adjective must be one of: Royal, Grand, Luxe, Imperial, Majestic, Elite, Noble, Crown\n"
            f"Feature must be one of: Summit, Azure, Palms, Heights, Gardens\n"
            f"Examples:\n"
            f"Noble Summit Villa\n"
            f"Grand Azure Apartment\n"
            f"Imperial Palms Estate\n"
            f"Your generated name:"
        )
        
        inputs = flan_tokenizer(prompt, return_tensors="pt", max_length=128, truncation=True)
        
        outputs = flan_model.generate(
            inputs.input_ids,
            max_length=20,
            num_return_sequences=1,
            temperature=0.9,  # Increased for more variety
            do_sample=True,
            no_repeat_ngram_size=2,
            num_beams=3,
            min_length=5
        )
        
        name = flan_tokenizer.decode(outputs[0], skip_special_tokens=True).strip()
        
        adjectives = [
            # Luxury/Elite
            'royal', 'grand', 'luxe', 'imperial', 'majestic', 'elite', 'noble', 'crown',
            'premium', 'sovereign', 'regal', 'paramount', 'opulent', 'elegant', 'pristine',
            'refined', 'exclusive', 'prime', 'luxury', 'stellar', 'supreme', 'celestial',
            'classical', 'superior', 'prestigious', 'eminent', 'divine', 'magnificent',
            'splendid', 'distinguished', 'exquisite', 'masterful', 'premier', 'grandiose',
            'aristocratic', 'chic', 'deluxe', 'gilded', 'lavish', 'ornate', 'palatial', 'plush',
            'posh', 'ritzy', 'sumptuous', 'swanky', 'upscale', 'venerated', 'vogue', 'world-class',
            'boutique', 'haute', 'rarefied', 'sophisticated', 'unparalleled', 'unrivaled', 'vanguard',
            'bespoke', 'custom', 'tailored', 'high-end', 'first-rate', 'top-tier', 'gold-standard',
            'iconic', 'legendary', 'mythic', 'hallowed', 'celebrated', 'renowned', 'fabled',
            'glamorous', 'dazzling', 'radiant', 'gleaming', 'lustrous', 'shimmering', 'sparkling',

            # Quality/Status
            'pinnacle', 'zenith', 'summit', 'apex', 'ultimate', 'premium', 'superior',
            'refined', 'distinctive', 'privileged', 'prestigious', 'elevated', 'masterful',
            'signature', 'legendary', 'innovative', 'timeless', 'transcendent', 'unique',
            'unmatched', 'peerless', 'nonpareil', 'unsurpassed', 'foremost', 'leading', 'dominant',
            'preeminent', 'paramount', 'quintessential', 'archetypal', 'definitive', 'model',
            'exemplary', 'ideal', 'perfect', 'flawless', 'immaculate', 'pristine', 'spotless',
            'impeccable', 'unblemished', 'untainted', 'unspoiled', 'untarnished', 'unadulterated',
            'authentic', 'genuine', 'bona fide', 'legitimate', 'certified', 'accredited', 'official',

            # Architectural
            'classic', 'heritage', 'landmark', 'monumental', 'palatial', 'stately',
            'architectural', 'baronial', 'castellated', 'columned', 'domed', 'magnificent',
            'neoclassical', 'gothic', 'romanesque', 'art deco', 'art nouveau', 'brutalist', 'futuristic',
            'minimalist', 'postmodern', 'rustic', 'mediterranean', 'tuscan', 'spanish', 'moroccan',
            'oriental', 'exotic', 'eclectic', 'avant-garde', 'cutting-edge', 'state-of-the-art',
            'symmetrical', 'ornamental', 'sculptural', 'monolithic', 'fortified', 'sprawling', 'towering',

            # Cultural/Historical
            'renaissance', 'victorian', 'georgian', 'regency', 'colonial', 'tudor',
            'venetian', 'florentine', 'byzantine', 'baroque', 'modern', 'contemporary',
            'ancient', 'medieval', 'feudal', 'enlightenment', 'industrial', 'modernist',
            'post-industrial', 'revolutionary', 'traditional', 'folkloric', 'mythological', 'tribal',
            'indigenous', 'ethnic', 'cosmopolitan', 'global', 'international', 'continental', 'exotic',

            # Emotional/Evocative
            'serene', 'tranquil', 'peaceful', 'calm', 'harmonious', 'balanced', 'soothing', 'relaxing',
            'invigorating', 'inspiring', 'uplifting', 'enchanting', 'captivating', 'mesmerizing',
            'alluring', 'enticing', 'breathtaking', 'stunning', 'awe-inspiring', 'majestic', 'grandiose'
        ]
        
        features = [
            # Geographic
            'summit', 'azure', 'palms', 'heights', 'gardens', 'oasis', 'meadows', 'springs',
            'grove', 'forest', 'lagoon', 'marina', 'isle', 'beach', 'river', 'lakes',
            'valley', 'hills', 'glen', 'creek', 'brook', 'cliffs', 'woods', 'park',
            'ridge', 'haven', 'harbor', 'bay', 'cove', 'point', 'peninsula', 'shores',
            'plateau', 'canyon', 'ravine', 'gorge', 'fjord', 'delta', 'estuary', 'wetlands',
            'marsh', 'savannah', 'prairie', 'tundra', 'desert', 'dunes', 'volcano', 'crater',
            'geyser', 'hot springs', 'waterfront', 'seaside', 'coastline', 'headland', 'promontory',
            'strait', 'channel', 'archipelago', 'atoll', 'reef', 'jungle', 'rainforest', 'wilderness',
            'outback',

            # Architectural
            'towers', 'plaza', 'estate', 'manor', 'court', 'palace', 'pavilion', 'arcade',
            'terrace', 'villa', 'residences', 'mansion', 'sanctuary', 'galleria', 'colonnade',
            'chateau', 'gates', 'square', 'mews', 'quarters', 'promenade', 'commons',
            'arcade', 'boulevard', 'rotunda', 'portico', 'veranda', 'courtyard', 'atrium',
            'obelisk', 'spire', 'dome', 'turret', 'bastion', 'rampart', 'citadel', 'fortress',
            'keep', 'bungalow', 'chalet', 'lodge', 'cabin', 'farmhouse', 'windmill', 'lighthouse',
            'aqueduct', 'viaduct', 'bridge', 'tunnel', 'amphitheater', 'coliseum', 'stadium',

            # Natural Elements
            'sunrise', 'sunset', 'dawn', 'dusk', 'horizon', 'vista', 'panorama', 'skyline',
            'meadow', 'garden', 'grove', 'orchard', 'commons', 'green', 'park', 'woods',
            'forest', 'springs', 'waters', 'stream', 'brook', 'creek', 'falls', 'cascade',
            'waterfall', 'rapids', 'whirlpool', 'geyser', 'hot spring', 'glacier', 'iceberg',
            'coral reef', 'mangrove', 'wetland', 'marshland', 'swamp', 'bog', 'fen', 'peatland',

            # Premium Materials
            'marble', 'ivory', 'crystal', 'pearl', 'emerald', 'sapphire', 'jade', 'amber',
            'ruby', 'golden', 'silver', 'platinum', 'diamond', 'bronze', 'copper', 'quartz',
            'granite', 'onyx', 'coral', 'jasper', 'topaz', 'garnet', 'opal', 'beryl',
            'obsidian', 'malachite', 'lapis lazuli', 'turquoise', 'agate', 'amethyst', 'citrine',
            'moonstone', 'zircon', 'spinel', 'peridot', 'tanzanite', 'aquamarine', 'bloodstone',

            # Directional/Positional
            'north', 'south', 'east', 'west', 'central', 'upper', 'lower', 'mid',
            'corner', 'cross', 'center', 'edge', 'rim', 'crown', 'peak', 'crest',
            'foothills', 'plateau', 'basin', 'divide', 'watershed', 'headwaters', 'mouth',
            'confluence', 'tributary', 'meander', 'oxbow', 'reservoir', 'dam', 'levee',

            # Seasonal/Environmental
            'spring', 'summer', 'autumn', 'winter', 'solstice', 'equinox', 'twilight',
            'dawn', 'mist', 'fog', 'rain', 'snow', 'frost', 'ice', 'breeze', 'wind',
            'monsoon', 'typhoon', 'hurricane', 'tornado', 'cyclone', 'blizzard', 'avalanche',
            'landslide', 'earthquake', 'volcano', 'eruption', 'lava', 'ash', 'crater'
        ]
        
        # Clean and validate name
        words = name.strip().split()
        if len(words) >= 2:
            if words[0].lower() in adjectives:
                name = f"{words[0].capitalize()} {words[1].capitalize()} {property_type}"
            else:
                name = f"{random.choice(adjectives).capitalize()} {random.choice(features).capitalize()} {property_type}"
        else:
            name = f"{random.choice(adjectives).capitalize()} {random.choice(features).capitalize()} {property_type}"
            
        return name

    except Exception as e:
        print(f"Error generating property name: {e}")
        return f"{random.choice(['Grand', 'Elite', 'Majestic'])} Summit {property_type}"

# Function to generate random amenities for a property
def get_random_amenities():
    """Generate random selection of amenities for a property"""
    return {
        'internal': random.sample(amenities['internal'], k=random.randint(4, 8)),
        'external': random.sample(amenities['external'], k=random.randint(3, 6)),
        'nearby': random.sample(amenities['nearby'], k=random.randint(3, 5))
    }

# Function to write amenities as SQL INSERT statements to file
def insert_amenities(conn, amenities_dict, listing_id, user_id):
    type_display = {
        'internal': 'Internal Amenities',
        'external': 'External Amenities', 
        'nearby': 'Nearby Amenities'
    }
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        cursor = conn.cursor()
        
        # Get next available ID
        cursor.execute("SELECT MAX(id) FROM amenities")
        max_id = cursor.fetchone()[0] or 0
        next_id = max_id + 1
        
        # Prepare all amenities for batch insert
        amenities_data = []
        for amenity_type, amenities in amenities_dict.items():
            for amenity in amenities:
                amenities_data.append((
                    next_id,
                    type_display[amenity_type],
                    amenity,
                    listing_id,
                    user_id,
                    timestamp,
                    timestamp
                ))
                next_id += 1
        
        # Batch insert with ID field
        cursor.executemany("""
            INSERT INTO amenities_dataset (id, `type`, amenity, listing_id, user_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, amenities_data)
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"Error inserting amenities: {e}")
        raise
    finally:
        cursor.close()

# Function to format amenities into natural language for prompt
def format_amenities_prompt(selected_amenities):
    """Format amenities into natural language for prompt"""
    return (
        f"Interior features include {', '.join(selected_amenities['internal'])}. "
        f"External amenities include {', '.join(selected_amenities['external'])}. "
        f"Nearby facilities include {', '.join(selected_amenities['nearby'])}."
    )

# Generate a full dataset
def prepare_static_data():
    """Prepare static data used for generation"""
    return {
        'categories': ["Sale", "Rent"],
        'counties': ["Nairobi County", "Mombasa County", "Kisumu County", "Nakuru County"],
        'divisions': ["Kilimani", "Westlands", "Kileleshwa", "Karen", "Langata", "Kawangware"],
        'types': ["Apartment", "Villa", "Townhouse", "Penthouse", "Studio", "House", "New Development", "Cottage", "Duplex"],
        'classes': ["Luxury", "Regular", "Affordable"],
        'furnishings': ["Furnished", "Unfurnished", "Partially Furnished"],
        'bedrooms': [1, 2, 3, 4, 5],
        'bathrooms': [1, 2, 3, 4],
        'viewing_fees': [0, 1000, 2000, 3000, 5000],
        'statuses': ["Published", "Draft"],
        'availabilities': ["Available", "Unavailable"],
        'complex_ids': [33, 34, None],
        'user_ids': [4, 5, 6, 7],
        'currencies': ["KES", "USD"]
    }

# Generate a single listing with all required fields
async def generate_single_listing(static_data, start_date, end_date):
    """Generate a single listing with all required fields"""
    try:
        type_ = random.choice(static_data['types'])
        title = generate_property_names(type_) 
        division = random.choice(static_data['divisions'])
        class_ = random.choice(static_data['classes'])
        county = random.choice(static_data['counties'])
        
        # Generate amenities
        selected_amenities = get_random_amenities()
        amenities_text = format_amenities_prompt(selected_amenities)

        # Generate descriptions concurrently
        location_prompt = f"Write a short and engaging description for the location of a property in {division}: "
        property_prompt = f"Describe the property called {title} a {class_} {type_} in {county} with amenities such as {amenities_text}: "
        
        # Use Flan-T5 with GPT-2 fallback
        location_description = generate_text_flan(location_prompt)[0]
        if "error" in location_description.lower() or "failed" in location_description.lower():
            location_description = generate_text_gpt2(location_prompt)[0]
                
        property_description = generate_text_flan(property_prompt)[0]
        if "error" in property_description.lower() or "failed" in property_description.lower():
            property_description = generate_text_gpt2(property_prompt)[0]

        # Generate other fields
        category = random.choice(static_data['categories'])
        return (
            title,
            ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=10)),  # ref
            title.lower().replace(" ", "-").replace(",", "").replace(".", ""),  # slug
            category,
            county,
            division,  # county_specific
            round(random.uniform(36.70, 36.90), 6),  # longitude
            round(random.uniform(-1.40, -1.20), 6),  # latitude
            location_description,
            type_,
            class_,
            random.choice(static_data['furnishings']) if type_ != "Land" else None,  # furnishing
            random.choice(static_data['bedrooms']) if type_ != "Land" else None,  # bedroom
            random.choice(static_data['bathrooms']) if type_ != "Land" else None,  # bathroom
            generate_sq_area(type_),
            generate_amount(category),
            random.choice(static_data['viewing_fees']),
            property_description,
            random.choice(static_data['statuses']),
            random.choice(static_data['availabilities']),
            None,  # subscription_status
            random.choice(static_data['complex_ids']),
            random.choice(static_data['user_ids']),
            random_date(start_date, end_date).strftime("%Y-%m-%d %H:%M:%S"),  # created_at
            (random_date(start_date, end_date) + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S"),  # updated_at
            None,  # link
            random.choice(static_data['currencies'])
        ), selected_amenities
    except Exception as e:
        print(f"Error generating listing: {e}")
        return None

# Generate a dataset with optimized batch processing
async def generate_dataset(num_listings=5, batch_size=50):
    """Generate dataset with optimized batch processing"""
    print(f"Starting dataset generation for {num_listings} listings...")
    start_time = datetime.now()
    
    # Create connection pool
    pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name="listing_pool",
        pool_size=5,
        **MYSQL_CONFIG
    )
    
    # Prepare static data and dates
    static_data = prepare_static_data()
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)
    
    try:
        # Process in batches
        for batch_start in range(0, num_listings, batch_size):
            batch_end = min(batch_start + batch_size, num_listings)
            print(f"\nGenerating batch {batch_start + 1} to {batch_end}...")

            # Reset counter for new batch
            generation_counter['flan'] = 0
            generation_counter['gpt2'] = 0
            
            # Generate batch of listings concurrently
            tasks = [generate_single_listing(static_data, start_date, end_date) 
                    for _ in range(batch_end - batch_start)]
            batch_results = await asyncio.gather(*tasks)
            
            # Filter out failed generations
            # valid_listings = [l for l in batch_listings if l is not None]

            # Unpack listings and amenities, filter None results
            valid_results = [r for r in batch_results if r is not None]
            valid_listings = [r[0] for r in valid_results]
            listings_amenities = [r[1] for r in valid_results]
            
            # Batch insert into database
            if valid_listings:
                conn = pool.get_connection()
                try:
                    cursor = conn.cursor()
                    cursor.executemany("""
                        INSERT INTO listings_datasets 
                        (name, ref, slug, category, county, county_specific, 
                         longitude, latitude, location_description, type, class, 
                         furnishing, bedrooms, bathrooms, sq_area, amount, 
                         viewing_fee, property_description, status, availability, 
                         subscription_status, complex_id, user_id, created_at, 
                         updated_at, link, currency) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                               %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, valid_listings)

                    first_id = cursor.lastrowid
                    conn.commit()

                    # Use original amenities for each listing
                    for idx, (listing, amenities) in enumerate(zip(valid_listings, listings_amenities)):
                        listing_id = first_id + idx
                        user_id = listing[22]
                        insert_amenities(conn, amenities, listing_id, user_id)  # Fixed: Pass connection first
                        
                        
                except Exception as e:
                    print(f"Database error: {e}")
                finally:
                    cursor.close()
                    conn.close()
            
            print(f"Processed {len(valid_listings)} listings in current batch")
            
        end_time = datetime.now()
        print(f"\nFinished generating {num_listings} listings.")
        print(f"Total time taken: {end_time - start_time}")
        
    except Exception as e:
        print(f"Error during dataset generation: {e}")
    finally:
        pool._remove_connections()

if __name__ == "__main__":
    try:
        os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
        asyncio.run(generate_dataset(num_listings=2000, batch_size=50))
    except KeyboardInterrupt:
        print("\n  Process interrupted by user. Cleaning up...")
        sys.exit(0)
