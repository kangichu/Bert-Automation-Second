import pymysql
import logging
from config import MYSQL_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_data_from_mysql():
    """Fetch property listings from MySQL and return as a list of dictionaries."""
    query = """
        SELECT 
            users.first_name, 
            users.last_name,
            businesses.business_name
            businesses.account_type,
            businesses.business_email,
            listings.id, listings.name, listings.ref, listings.slug, listings.category, 
            listings.county, listings.county_specific, listings.longitude, listings.latitude, 
            listings.location_description, listings.type AS listing_type, listings.class AS listing_class, 
            listings.furnishing, listings.bedrooms, listings.bathrooms, listings.sq_area, listings.amount, 
            listings.viewing_fee, listings.property_description, listings.status, listings.availability, 
            listings.subscription_status, listings.complex_id, listings.user_id, listings.created_at, 
            listings.updated_at, listings.link, listings.currency, 
            complexes.title AS complex_title, complexes.ref_code AS complex_ref_code, complexes.slug AS complex_slug, 
            complexes.email AS complex_email, complexes.mobile AS complex_mobile, complexes.description AS complex_description, 
            complexes.type AS complex_type, complexes.class AS complex_class, complexes.county AS complex_county, 
            complexes.county_specific AS complex_county_specific, complexes.longitude AS complex_longitude, 
            complexes.latitude AS complex_latitude, complexes.location_description AS complex_location_description, 
            complexes.available AS complex_available, 
            GROUP_CONCAT(CONCAT(amenities.type, ': ', amenities.amenity) SEPARATOR '; ') AS amenities
        FROM listings 
        LEFT JOIN complexes ON listings.complex_id = complexes.id 
        LEFT JOIN amenities ON listings.id = amenities.listing_id
        LEFT JOIN users ON listings.user_id = users.id
        LEFT JOIN businesses ON users.id = businesses.user_id
        WHERE listings.status = 'Published'
        GROUP BY listings.id;
    """

    try:
        logging.info("Connecting to MySQL database...")
        
        # Use context manager to ensure the connection is properly closed
        with pymysql.connect(**MYSQL_CONFIG) as mysql_conn:
            with mysql_conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

        logging.info(f"Fetched {len(rows)} rows from MySQL.")

        # Extract column names from cursor description
        column_names = [desc[0] for desc in cursor.description]
        
        # Convert to list of dictionaries
        listings = [dict(zip(column_names, row)) for row in rows]

        return listings

    except Exception as e:
        logging.error(f"Error fetching data from MySQL: {e}")
        raise

