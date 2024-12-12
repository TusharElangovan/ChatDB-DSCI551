# ChatDB-DSCI551

## Project Overview
ChatDB is a versatile query-generation tool designed to bridge the gap between natural language and database operations. It supports both SQL and NoSQL databases, enabling users to interact with their data seamlessly. In addition to query translation, ChatDB allows users to **connect to SQL and MongoDB servers**, **upload and fetch data**, and even **generate random query suggestions** based on schema and data types.

## Features

### Files
- **Main Driver.py: 
  - Contains the code that handles both SQL and MongoDB query generation and execution.
- **FinalSQLFlow.py:
  - Handles the SQl Part of the program, converting user's natural language to SQL Queries, random query       generation, and direct SQL query execution
- **FinalNoSQLFlow.py:
  - Handles the MongoDB Part of the program, converting user's natural language to MongoDB Queries, random     query generation,n and direct SQL query execution

### Core Features
- **Natural Language Query Parsing:**
  - Converts natural language queries into SQL and NoSQL queries.
  - Handles complex operations like filtering, joins, and aggregations.

- **SQL Query Generation:**
  - Supports relational database operations such as `SELECT`, `JOIN`, `WHERE`, `GROUP BY`, and `ORDER BY.`
  - Compatible with MySQL and MongoDB.

- **NoSQL Query Generation:**
  - Generates NoSQL queries for MongoDB, including `$match,` `$group,` and `$project` operations.

- **Schema Matching and Optimization:**
  - Uses semantic matching to align user input with database schema.
  - Recommends indexes to optimize database performance.

### New Features
- **Database Connectivity:**
  - Allows users to connect to SQL and MongoDB servers via configuration.
  - Supports secure authentication and database selection.

- **Data Upload and Fetch:**
  - Enables users to upload datasets (e.g., CSV files) to the connected databases.

- **Random Query Suggestions:**
  - Analyzes schema and data types to suggest meaningful queries.
  - Example:
    - For a table with `date_of_birth`, suggests: *"select all date_of_birth after 1990."*
    - For a collection with `price`, suggests: *"Show price above $50."*

- **Interactive Query Testing:**
  - Allows users to execute queries directly and view the results in a console or web interface.

## Key Technologies
- **Programming Language:** Python
- **Libraries and Frameworks:**
  - `NLTK`, `spaCy`: For natural language processing.
  - `PyMongo`: For MongoDB interaction.
  - `Pandas`: For dataset handling and manipulation.
- **Databases:** MySQL, MongoDB

## How It Works

1. **Database Connection:**
   - ChatDB establishes a connection to the specified SQL or MongoDB server.

2. **Data Interaction:**
   - Upload data using the `upload_data` function, which accepts files like CSV and maps them to the database schema.
   - Fetch data using natural language or generated queries.

3. **Query Generation:**
   - User inputs a query in natural language.
   - ChatDB translates the query into SQL/NoSQL syntax, taking schema and data types into account.

4. **Random Query Suggestions:**
   - ChatDB scans the schema for columns like dates, numbers, and text to suggest useful queries.

5. **Execution and Results:**
   - Queries can be executed directly from the tool, and results are displayed in tabular or JSON format.

## Download and Installation Instructions

### Prerequisites
- Python 3.8 or higher
- MySQL Server (for SQL database operations)
- MongoDB (for NoSQL database operations)

### Steps to Download and Install

1. **Clone the Repository:**
   Open your terminal or command prompt and run the following command:
   ```bash
   git clone https://github.com/TusharElangovan/ChatDB-DSCI551.git
   
