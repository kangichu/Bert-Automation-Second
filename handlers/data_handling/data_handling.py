import logging
import numpy as np

def format_data(listings):
    """Generate narratives for multiple property listings."""

    if not listings:
        logging.warning("No listings found to generate narratives.")
        return None

    listing_ids = []
    narratives = []
    logging.info("Loading data formatter...")

    for listing in listings:
        # Define the keys you want to unpack in the order you want them
        keys = [
            'id', 'name', 'ref', 'slug', 'category', 'county', 'county_specific', 'longitude', 'latitude', 'location_description',
            'listing_type', 'listing_class', 'furnishing', 'bedrooms', 'bathrooms', 'sq_area', 'amount', 'viewing_fee',
            'property_description', 'status', 'availability', 'subscription_status', 'complex_id', 'user_id',
            'created_at', 'updated_at', 'link', 'currency', 'amenities', 'complex_title', 'complex_slug', 'complex_email', 'complex_mobile', 'complex_description',
            'complex_type', 'complex_class', 'complex_county', 'complex_county_specific', 'complex_longitude',
            'complex_latitude', 'complex_location_description', 'complex_available', 
            'first_name', 'last_name', 'business_name', 'account_type', 'business_email'
        ]

        # Unpack the values from the dictionary into variables
        id, name, ref, slug, category, county, county_specific, longitude, latitude, location_description, \
        listing_type, listing_class, furnishing, bedrooms, bathrooms, sq_area, amount, viewing_fee, \
        property_description, status, availability, subscription_status, complex_id, user_id, \
        created_at, updated_at, link, currency, amenities, complex_title, complex_slug, complex_email, complex_mobile, complex_description, \
        complex_type, complex_class, complex_county, complex_county_specific, complex_longitude, \
        complex_latitude, complex_location_description, complex_available, \
        first_name, last_name, business_name, account_type, business_email = [listing.get(key, None) for key in keys]

        # Now you can use these variables directly
        combined_text = []

        # Combine key details into a single text entry per listing
        combined_text.append(
            f"Property Name: {name} Property Ref: {ref} Type: {listing_type},\n"
            f"Property Manager: {business_name} under User:{first_name} {last_name} Contact Email: {business_email}"
        )

        if listing_class == 'Luxury':
            combined_text.append(f"This is a high end {listing_class} {listing_type}.")
        
        combined_text.append(
            f"Category: {category},\n"
            f"Location: {county_specific}, {county}\n"
            f"Coordinates: Longitude {longitude}, Latitude {latitude}\n"
            f"Details: {complex_description}"
        )

        # Bedrooms and Bathrooms
        if bedrooms:
            combined_text.append(f"It features {bedrooms} {'bedroom' if bedrooms == '1' else 'bedrooms'}.")
        if bathrooms:
            combined_text.append(f"And includes {bathrooms} {'bathroom' if bathrooms == 1 else 'bathrooms'}.")

        # Furnishing
        if furnishing:
            combined_text.append(f"The property is {furnishing.lower()}.")

        # Square Area
        if sq_area:
            combined_text.append(f"With a total area of {sq_area} sq. ft., it offers ample space.")

        # Complex Information
        if complex_id:
            combined_text.append(f"{name} is part of the prestigious {complex_class} : {complex_title} complex of Type: {complex_type}")
        else:
            combined_text.append(f"{name} stands as an independent property.")

        if amenities:
            amenity_list = amenities.split('; ')
            for amenity in amenity_list:
                amenity_type, amenity_detail = amenity.split(': ')
                if amenity_type == 'Nearby Amenities':
                    combined_text.append(f"Enjoy the convenience of {amenity_detail} just a short distance away.")
                elif amenity_type == 'External Amenities':
                    combined_text.append(f"Take advantage of {amenity_detail} right on the property's grounds.")
                elif amenity_type == 'Internal Amenities':
                    combined_text.append(f"Inside, you'll find {amenity_detail} for your comfort and enjoyment.")

        # Category
        if category.lower() == 'rent':
            combined_text.append(f"This property is available for rent at {currency} {amount} per month.")
        else:
            combined_text.append(f"This property is up for sale at {currency} {amount}.")

        # Viewing Fee
        if viewing_fee:
            combined_text.append(f"A viewing fee of {viewing_fee} is required to schedule a visit.")
        
        listing_ids.append(id)  # Keep track of listing IDs

        # Combine all parts into a single narrative
        full_narrative = ' '.join(combined_text)
        
        # Append the generated narrative to the list of narratives
        narratives.append((id, full_narrative))

    logging.info(f"Generated narratives for {len(narratives)} rows.")

    return narratives, np.array(listing_ids)
    
