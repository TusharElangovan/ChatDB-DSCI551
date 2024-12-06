# ChatDB-DSCI551
## Project Overview
ChatDB is a versatile query-generation tool designed to bridge the gap between natural language and database operations. It supports both SQL and NoSQL databases, enabling users to interact with their data seamlessly. In addition to query translation, ChatDB allows users to **connect to SQL and MongoDB servers**, **upload and fetch data**, and even **generate random query suggestions** based on schema and data types.

## Features

### Core Features
- **Natural Language Query Parsing:**
  - Converts natural language queries into SQL and NoSQL queries.
  - Handles complex operations like filtering, joins, and aggregations.

- **SQL Query Generation:**
  - Supports relational database operations such as `SELECT`, `JOIN`, `WHERE`, `GROUP BY`, and `ORDER BY`.
  - Compatible with MySQL, PostgreSQL, and other relational databases.

- **NoSQL Query Generation:**
  - Generates NoSQL queries for MongoDB, including `$match`, `$group`, and `$project` operations.

- **Schema Matching and Optimization:**
  - Uses semantic matching to align user input with database schema.
  - Recommends indexes to optimize database performance.

- **Error Handling and Suggestions:**
  - Detects errors in user input and provides actionable suggestions.

### New Features
- **Database Connectivity:**
  - Allows users to connect to SQL and MongoDB servers via configuration.
  - Supports secure authentication and database selection.

- **Data Upload and Fetch:**
  - Enables users to upload datasets (e.g., CSV files) to the connected databases.
  - Fetches data in real-time for visualization or further analysis.

- **Random Query Suggestions:**
  - Analyzes schema and data types to suggest meaningful queries.
  - Example:
    - For a table with `date_of_birth`, suggests: *"Find all entries born after 1990."*
    - For a collection with `price`, suggests: *"Retrieve items priced above $50."*

- **Interactive Query Testing:**
  - Allows users to execute queries directly and view the results in a console or web interface.

## Key Technologies
- **Programming Language:** Python
- **Libraries and Frameworks:**
  - `NLTK`, `spaCy`: For natural language processing.
  - `SQLAlchemy`: For SQL query construction and execution.
  - `PyMongo`: For MongoDB interaction.
  - `Pandas`: For dataset handling and manipulation.
- **Databases:** MySQL, PostgreSQL, MongoDB
