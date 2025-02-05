Below is a detailed, step-by-step description of what the code is doing:

1. **Imports and Environment Setup**  
   - The code begins by importing various Python modules needed for its functionality. These include standard libraries (such as `os`, `sys`, `random`, `datetime`, and `asyncio`), third-party libraries (like `mysql.connector` for MySQL database connections, `dotenv` for loading environment variables, and the Hugging Face Transformers library for working with GPT-2 and Flan-T5), and the OpenAI library for accessing GPT-3.5.
   - It then loads environment variables from a `.env` file using the `dotenv` package. This allows sensitive information (like API keys and database credentials) to be managed outside of the source code.
   - An environment variable is set to disable oneDNN custom operations (used by TensorFlow) by setting `TF_ENABLE_ONEDNN_OPTS` to `'0'`.

2. **Initializing the OpenAI Client**  
   - The script attempts to initialize an OpenAI client by retrieving the API key from the environment variables. If the API key is missing or an error occurs during initialization, the script prints an error message and exits. This client is later used to generate text with OpenAI’s GPT-3.5 model.

3. **Database Configuration**  
   - A dictionary (`MYSQL_CONFIG`) is defined with the MySQL database connection parameters. These parameters (host, username, password, database, and port) are read from the environment variables, allowing the code to connect to the desired database.

4. **Defining Property-Related Data**  
   - **Price Ranges:** A dictionary defines the minimum and maximum price values for properties based on their category (either "Sale" or "Rent").
   - **Property Name Mapping:** Another dictionary maps various property types (like Apartment, Villa, Townhouse, etc.) to lists of potential property names. These names are later used to randomly assign a title to a listing.
   - **Square Area Ranges:** Similarly, a dictionary maps property types to their potential square area ranges. These values are used to generate a realistic square footage for each property listing.
   - **Amenities:** A dictionary defines internal, external, and nearby amenities that can be randomly assigned to properties.

5. **Loading GPT-2 and Flan-T5 Models and Tokenizers**  
   - The script loads pre-trained GPT-2 and Flan-T5 models and their tokenizers from Hugging Face. These provide alternative text generation methods (in case the OpenAI GPT-3.5 API fails due to rate limits or errors).
   - The tokenizer’s padding token is set to the end-of-sequence token to avoid warnings during text generation.

6. **Text Generation Functions**  
   - **Using Flan-T5:** A function is defined to generate text using the Flan-T5 model. It encodes a given prompt, creates an attention mask, and then uses the model to generate text. The parameters such as `max_length`, `temperature`, `top_p`, and `repetition_penalty` are set to control the randomness and coherence of the generated text. If an error occurs, it returns a fallback message.
   - **Using GPT-2:** Another function is defined to generate text using the GPT-2 model. It follows a similar process to the Flan-T5 function but uses different parameters to control the text generation.
   - **Using OpenAI's GPT-3.5:** An asynchronous function is created to generate text using OpenAI’s GPT-3.5. It sends a prompt to the API, waits for the response, and returns the generated text. In case of a rate limit or other API-related errors, it catches those exceptions and returns `None`, which signals that a fallback (such as Flan-T5 or GPT-2) should be used.

7. **Utility Functions**  
   - **Random Date Generation:** A function generates a random date between a provided start and end date. This is useful for assigning realistic creation and update timestamps to each listing.
   - **MySQL Connection:** A function establishes a connection to the MySQL database using the provided configuration. If the connection fails, it logs an error and exits.
   - **Creating the Database Table:** Another function checks for the existence of a specific table (`listings_datasets`) in the database and creates it if it does not exist. This table is structured with fields corresponding to property listing attributes (e.g., name, reference code, location details, description, pricing, and timestamps).
   - **Inserting a Listing:** A function is defined to insert a single listing record into the database table. It uses a prepared SQL statement with placeholders to insert the data safely. If insertion fails, it rolls back the transaction.

8. **Data Generation Helpers**  
   - **Generating a Random Amount:** A helper function generates a random monetary amount based on the category (“Sale” or “Rent”) by using the pre-defined price ranges.
   - **Generating a Random Square Area:** Similarly, another helper generates a random square area for the property based on its type.
   - **Generating a Random Property Name:** This function generates a random property name using the Flan-T5 model or manually from the property name mapping.
   - **Generating Random Amenities:** A function generates a random selection of internal, external, and nearby amenities for a property.
   - **Inserting Amenities:** A function inserts the generated amenities into the database.
   - **Formatting Amenities Prompt:** This function formats the selected amenities into a natural language prompt for text generation.

9. **Dataset Generation Function**  
   - An asynchronous function is defined to generate a dataset of property listings. This function drives the main logic:
     - **Initial Setup:** It starts by printing the number of listings to generate and records the start time. It also defines sample lists for categories, counties, divisions (neighborhoods), property types, classes (e.g., Luxury, Regular), furnishing options, bedroom and bathroom counts, viewing fees, status, availability, and user IDs.
     - **Database Connection and Table Creation:** It establishes a connection to the MySQL database and ensures that the target table exists.
     - **Looping Through Listings:** For each listing to be generated:
       - A title is generated by selecting a random property name based on a random property type.
       - A location description is generated by constructing a prompt (including a randomly chosen division) and attempting to generate text using Flan-T5. If Flan-T5 fails, it falls back to GPT-2.
       - A property description is similarly generated using a prompt that includes a random class, property type, and county. Again, it falls back to GPT-2 if necessary.
       - Other fields such as a random reference code, a URL-friendly slug, category, county, longitude, latitude, type, class, furnishing, bedroom count, bathroom count, square area, price amount, viewing fee, status, availability, and currency are generated using a mix of random choices and the helper functions defined earlier.
       - Timestamps for `created_at` and `updated_at` are generated by selecting random dates within the year 2025.
       - A tuple of all these values is constructed to represent a single listing.
       - This listing is then inserted into the MySQL database.
       - The generated amenities are inserted into the database.
       - A progress message is printed every 50 listings generated.
     - **Final Steps:** Once all listings have been generated, the function prints the end time, calculates the total time taken, and closes the database connection.

10. **Script Execution Entry Point**  
    - The final block checks if the script is being run directly (i.e., not imported as a module). If so, it uses `asyncio.run` to execute the asynchronous dataset generation function.
    - It also handles keyboard interruptions (like pressing Ctrl+C) gracefully by catching the `KeyboardInterrupt` exception, printing an interruption message, and exiting the script cleanly.

In summary, the code is designed to programmatically generate realistic property listings using both AI-generated text and random data. It combines external text-generation services (OpenAI’s GPT-3.5 with fallbacks to Flan-T5 and GPT-2) with structured random data (like prices, square footage, and dates), and stores the generated listings in a MySQL database. The generation of this will be used for training the FAISS index (FAISS IVFPQ index) before we can begin storage into the index.