# Guvi_CAPSTON_Project_YOUTUBE-DATA-HARVESTING-AND-WAREHOUSING

YouTube Data Harvesting and Warehousing is a comprehensive project that employs the Google API to extract valuable information from YouTube channels. The extracted data is stored in MongoDB, migrated to a SQL data warehouse, and made accessible for analysis within a user-friendly Streamlit application.

**Key Technologies and Skills**

PYTHON: Python is the primary language for the development of the entire application, covering data retrieval, processing, analysis, and visualization.

GOOGLE API CLIENT: Utilizing the googleapiclient library in Python to interact with YouTube's Data API v3, enabling the retrieval of essential information such as channel details, video specifics, and comments.

MONGODB: MongoDB, a document database, is used for its scalability and ability to handle evolving data schemas. It stores structured or unstructured data in a JSON-like format.

POSTGRESQL: PostgreSQL, an open-source and advanced database management system, is employed for its reliability and extensive features. It provides a platform for storing and managing structured data with support for various data types and advanced SQL capabilities.

STREAMLIT: The Streamlit library is used to create a user-friendly UI, allowing users to interact with the program, retrieve and analyze data seamlessly.

**REQUIRED LIBRARIES:**

googleapiclient.discovery
streamlit
psycopg2
pymongo
pandas

**FEATURES:**

Retrieve Data from YouTube API:

Fetch channel information, playlists, videos, and comments from the YouTube API.
MongoDB Storage:

Store the retrieved data in a MongoDB database, leveraging its document-oriented structure.
SQL Data Warehouse:

Migrate the stored data to a SQL data warehouse, utilizing the advanced features of PostgreSQL.
Streamlit and Plotly:

Utilize Streamlit and Plotly for data analysis and visualization, creating an interactive and user-friendly interface.
SQL Queries:

Perform queries on the SQL data warehouse to extract valuable insights into channel performance, video metrics, and more.
Getting Started

**Installation:**

Install the required libraries using the following command:

bash
Copy code
pip install googleapiclient streamlit psycopg2 pymongo pandas

**Configuration:**

Set up your Google API key in the script.

**Run the Application:**

Execute the Streamlit application:


streamlit run your_script.py

**Usage**

Enter a YouTube channel ID in the Streamlit app.
Click buttons to fetch, store, and analyze data.
Use the interface to visualize and query data stored in both MongoDB and the SQL data warehouse.

**Contributing**
Contributions to this project are welcome. Please follow the guidelines in the README to contribute effectively.
