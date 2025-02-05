import pymysql
import logging
from config import MYSQL_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define the function to fetch data from MySQL
def fetch_data_from_mysql():
    """Fetch property listings from MySQL and return as a list of dictionaries."""
    query = """
        SELECT 
            users.first_name, 
            users.last_name,
            businesses.business_name,
            businesses.account_type,
            businesses.business_email,
            listings_datasets.id, listings_datasets.name, listings_datasets.ref, listings_datasets.slug, listings_datasets.category, 
            listings_datasets.county, listings_datasets.county_specific, listings_datasets.longitude, listings_datasets.latitude, 
            listings_datasets.location_description, listings_datasets.type AS listing_type, listings_datasets.class AS listing_class, 
            listings_datasets.furnishing, listings_datasets.bedrooms, listings_datasets.bathrooms, listings_datasets.sq_area, listings_datasets.amount, 
            listings_datasets.viewing_fee, listings_datasets.property_description, listings_datasets.status, listings_datasets.availability, 
            listings_datasets.subscription_status, listings_datasets.complex_id, listings_datasets.user_id, listings_datasets.created_at, 
            listings_datasets.updated_at, listings_datasets.link, listings_datasets.currency, 
            complexes.title AS complex_title, complexes.ref_code AS complex_ref_code, complexes.slug AS complex_slug, 
            complexes.email AS complex_email, complexes.mobile AS complex_mobile, complexes.description AS complex_description, 
            complexes.type AS complex_type, complexes.class AS complex_class, complexes.county AS complex_county, 
            complexes.county_specific AS complex_county_specific, complexes.longitude AS complex_longitude, 
            complexes.latitude AS complex_latitude, complexes.location_description AS complex_location_description, 
            complexes.available AS complex_available, 
            GROUP_CONCAT(CONCAT(amenities_dataset.type, ': ', amenities_dataset.amenity) SEPARATOR '; ') AS amenities
        FROM listings_datasets 
        LEFT JOIN complexes ON listings_datasets.complex_id = complexes.id 
        LEFT JOIN amenities_dataset ON listings_datasets.id = amenities_dataset.listing_id
        LEFT JOIN users ON listings_datasets.user_id = users.id
        LEFT JOIN businesses ON users.id = businesses.user_id
        WHERE listings_datasets.status = 'Published'
        GROUP BY listings_datasets.id;
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

# Define the function to fetch new listings from MySQL
def fetch_new_listings(tracker):
    """Fetch only new property listings not already in FAISS index."""
    
    # Get tracked listing IDs
    tracked_ids = list(tracker.get_tracked_ids())
    
    # Modify query to exclude tracked listings
    query = """
        SELECT 
            users.first_name, 
            users.last_name,
            businesses.business_name,
            businesses.account_type,
            businesses.business_email,
            listings_datasets.id, listings_datasets.name, listings_datasets.ref, listings_datasets.slug, listings_datasets.category, 
            listings_datasets.county, listings_datasets.county_specific, listings_datasets.longitude, listings_datasets.latitude, 
            listings_datasets.location_description, listings_datasets.type AS listing_type, listings_datasets.class AS listing_class, 
            listings_datasets.furnishing, listings_datasets.bedrooms, listings_datasets.bathrooms, listings_datasets.sq_area, listings_datasets.amount, 
            listings_datasets.viewing_fee, listings_datasets.property_description, listings_datasets.status, listings_datasets.availability, 
            listings_datasets.subscription_status, listings_datasets.complex_id, listings_datasets.user_id, listings_datasets.created_at, 
            listings_datasets.updated_at, listings_datasets.link, listings_datasets.currency, 
            complexes.title AS complex_title, complexes.ref_code AS complex_ref_code, complexes.slug AS complex_slug, 
            complexes.email AS complex_email, complexes.mobile AS complex_mobile, complexes.description AS complex_description, 
            complexes.type AS complex_type, complexes.class AS complex_class, complexes.county AS complex_county, 
            complexes.county_specific AS complex_county_specific, complexes.longitude AS complex_longitude, 
            complexes.latitude AS complex_latitude, complexes.location_description AS complex_location_description, 
            complexes.available AS complex_available, 
            GROUP_CONCAT(CONCAT(amenities_dataset.type, ': ', amenities_dataset.amenity) SEPARATOR '; ') AS amenities
        FROM listings_datasets 
        LEFT JOIN complexes ON listings_datasets.complex_id = complexes.id 
        LEFT JOIN amenities_dataset ON listings_datasets.id = amenities_dataset.listing_id
        LEFT JOIN users ON listings_datasets.user_id = users.id
        LEFT JOIN businesses ON users.id = businesses.user_id
        WHERE listings_datasets.status = 'Published'
        AND listings_datasets.id NOT IN (%s)
        GROUP BY listings_datasets.id;
    """
    
    try:
        logging.info("Fetching new listings from MySQL...")
        
        with pymysql.connect(**MYSQL_CONFIG) as mysql_conn:
            with mysql_conn.cursor() as cursor:
                # Format query with tracked IDs or '0' if none exist
                formatted_query = query % ','.join(map(str, tracked_ids)) if tracked_ids else query.replace("NOT IN (%s)", "NOT IN (0)")
                cursor.execute(formatted_query)
                rows = cursor.fetchall()

        logging.info(f"Fetched {len(rows)} new listings_datasets.")

        column_names = [desc[0] for desc in cursor.description]
        listings = [dict(zip(column_names, row)) for row in rows]

        return listings

    except Exception as e:
        logging.error(f"Error fetching new listings: {e}")
        raise