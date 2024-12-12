# ChatDB-DSCI551

## Project Overview
ChatDB is a versatile query-generation tool that bridges the gap between natural language and database operations. It supports both SQL and NoSQL databases, enabling users to interact with their data seamlessly. In addition to query translation, ChatDB allows users to **connect to SQL and MongoDB servers**, **upload and fetch data**, and **generate random query suggestions** based on schema and data types.

## Features

### Files
- **Main Driver:**
  - Handles both SQL and MongoDB query generation and execution.
- **FinalSQLFlow:**
  - Converts natural language to SQL queries, generates random queries, and supports direct SQL query execution.
- **FinalNoSQLFlow:**
  - Converts natural language to MongoDB queries, generates random queries, and supports direct MongoDB query execution.

### Core Features
- **Natural Language Query Parsing:**
  - Converts natural language queries into SQL and NoSQL queries.
  - Supports complex operations such as filtering, joins, and aggregations.

- **SQL Query Generation:**
  - Supports relational database operations like `SELECT`, `JOIN`, `WHERE`, `GROUP BY`, and `ORDER BY`.
  - Compatible with MySQL and other SQL databases.

- **NoSQL Query Generation:**
  - Generates MongoDB queries with operations like `$match`, `$group`, and `$project`.

- **Schema Matching and Optimization:**
  - Aligns user input with the database schema using semantic matching.
  - Recommends indexes to improve database performance.

### New Features
- **Database Connectivity:**
  - Allows users to securely connect to SQL and MongoDB servers via configuration.
  - Supports database authentication and selection.

- **Data Upload and Fetch:**
  - Uploads datasets (e.g., CSV files) to connected databases.
  - Fetches data using natural language or generated queries.

- **Random Query Suggestions:**
  - Analyzes schema and data types to suggest meaningful queries.
  - Examples:
    - For a table with `date_of_birth`, suggests: *"Select all date_of_birth after 1990."*
    - For a collection with `price`, suggests: *"Show price above $50."*

- **Interactive Query Testing:**
  - Executes queries directly and displays results in a console or web interface.

## Key Technologies
- **Programming Language:** Python
- **Libraries and Frameworks:**
  - `NLTK`, `spaCy`: For natural language processing.
  - `PyMongo`: For MongoDB interaction.
  - `Pandas`: For dataset handling and manipulation.
- **Databases:** MySQL, MongoDB

## How It Works

1. **Database Connection:**
   - Establish a connection to the specified SQL or MongoDB server.

2. **Data Interaction:**
   - Upload data using the `upload_data` function, which maps files like CSV to the database schema.
   - Fetch data using natural language or generated queries.

3. **Query Generation:**
   - Input a query in natural language.
   - Translate the query into SQL/NoSQL syntax, considering schema and data types.

4. **Random Query Suggestions:**
   - Scan the schema for columns such as dates, numbers, and text to suggest useful queries.

5. **Execution and Results:**
   - Execute queries directly in the tool and view results in tabular or JSON format.

## Installation Instructions

### Prerequisites
- Python 3.8 or higher
- MySQL Server (for SQL database operations)
- MongoDB (for NoSQL database operations)

### Steps to Install

1. **Clone the Repository:**
   Open your terminal or command prompt and run the following command:
   ```bash
   git clone https://github.com/TusharElangovan/ChatDB-DSCI551.git
   ```

2. **Navigate to the Project Directory:**
   ```bash
   cd ChatDB-DSCI551
   ```

3. **Install Dependencies:**
   Use `pip` to install the required libraries:
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the Main Script:**
   Start the application by running:
   ```bash
   python main.py
   
