import sys
import nltk
import re
import os
import pandas as pd
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
import random
import mysql.connector
import pandas as pd
from mysql.connector import Error
import pprint

# Directly Execute SQL Queries
def execute_Sql_squery(cursor, query):
 
    # query = "Select name from ORG1000copy where founded = 1990;"

    try: 
        cursor.execute(query)
            # Fetch and print results if the query returns any
        columns = [column[0] for column in cursor.description]  # Get column names
        results = cursor.fetchall()
        
        # Print column headers
        print(f"{' | '.join(columns)}")
        print("-" * 40)
        
        # Print rows
        for row in results:
            print(" | ".join(str(value) for value in row))
        print("-" * 40)
    except:
        print("Incorrect SQL Syntax or Query, Please Try Again")

## -- This function will generate random queries by placing the col names in random order

## LOCAL FUNCTION -  when pushing file from local to sql server
def generate_sql_queries(file_path):
    column_names, column_types = extract_columns_and_types(file_path)

    if not column_names or len(column_names) < 2:
        raise ValueError("At least two columns are required to generate meaningful SQL queries.")

    table_name = extract_file_name(file_path)

    # Classify columns by data type
    int_float_columns = [col for col, dtype in column_types.items() if dtype in {"INT", "FLOAT"}]
    string_columns = [col for col, dtype in column_types.items() if dtype in {"VARCHAR(255)", "TEXT"}]

    # Helper function to pick a random column
    def pick_column(data_type_list):
        return random.choice(data_type_list).upper() if data_type_list else None

    # Generate queries with randomization for each run
    queries = {
        "select_all": f"SELECT * FROM {table_name};",
        # "select_columns": f"SELECT {', '.join(random.sample(column_names, len(column_names)))[:random.randint(1, len(column_names))].upper()} FROM {table_name};",
        "select_columns": f"SELECT {', '.join(random.sample(column_names, random.randint(1, len(column_names))))} FROM {table_name};",
        "group_by": f"SELECT {pick_column(string_columns)}, COUNT(*) FROM {table_name} GROUP BY {pick_column(string_columns)};"
        if string_columns else "Not applicable for this dataset.",
        "group_by_having": f"SELECT {pick_column(string_columns)}, AVG({pick_column(int_float_columns)}) FROM {table_name} "
                           f"GROUP BY {pick_column(string_columns)} HAVING AVG({pick_column(int_float_columns)}) > {random.randint(10, 100)};"
        if string_columns and int_float_columns else "Not applicable for this dataset.",
        "where_clause": f"SELECT {pick_column(int_float_columns)}, {pick_column(string_columns)} FROM {table_name} WHERE {pick_column(int_float_columns)} > {random.randint(50, 500)};"
        if int_float_columns else "Not applicable for this dataset.",
        "order_by": f"SELECT {pick_column(int_float_columns)}, {pick_column(string_columns)} FROM {table_name} ORDER BY {pick_column(int_float_columns)} DESC;"
        if int_float_columns else "Not applicable for this dataset.",
        "limit": f"SELECT {pick_column(column_names)}, {pick_column(column_names)} FROM {table_name} LIMIT {random.randint(5, 20)};",
        "join": f"SELECT a.{pick_column(string_columns)}, b.{pick_column(string_columns)} FROM {table_name} a "
                f"JOIN {table_name}_related b ON a.{pick_column(string_columns)} = b.{pick_column(string_columns)};"
        if string_columns else "Not applicable for this dataset.",
        "average": f"SELECT AVG({pick_column(int_float_columns)}) AS average_{pick_column(int_float_columns)} FROM {table_name};"
        if int_float_columns else "Not applicable for this dataset.",
        "max": f"SELECT MAX({pick_column(int_float_columns)}) AS max_{pick_column(int_float_columns)} FROM {table_name};"
        if int_float_columns else "Not applicable for this dataset.",
        "min": f"SELECT MIN({pick_column(int_float_columns)}) AS min_{pick_column(int_float_columns)} FROM {table_name};"
        if int_float_columns else "Not applicable for this dataset."
    }

    # Ensure at least one query for each required type
    required_queries = {
        "select_all", "select_columns", "group_by", "group_by_having",
        "where_clause", "order_by", "limit", "join", "average", "max", "min"
    }

    # Filter out invalid queries
    valid_queries = {key: query for key, query in queries.items() if "Not applicable" not in query}

    # Check if any required query type is missing
    for query_type in required_queries:
        if query_type not in valid_queries:
            valid_queries[query_type] = f"-- Could not generate a valid {query_type} query for the dataset."

    return valid_queries

## SERVER FUNCTION - when pulling file from server
def generate_Serversql_queries(cursor,table_name):
    column_names, column_types = get_table_columns(cursor,table_name)

    if not column_names or len(column_names) < 2:
        raise ValueError("At least two columns are required to generate meaningful SQL queries.")

    # table_name = extract_file_name(file_path)


    # Classify columns by data type - they are small because sql stores them that way
    int_float_columns = [col for col, dtype in column_types.items() if dtype in {"int", "float"}]
    string_columns = [col for col, dtype in column_types.items() if dtype in {"varchar(255)", "text"}]

    # Helper function to pick a random column
    def pick_column(data_type_list):
        return random.choice(data_type_list).upper() if data_type_list else None

    # Generate queries with randomization for each run
    queries = {
        "select_all": f"SELECT * FROM {table_name};",
        # "select_columns": f"SELECT {', '.join(random.sample(column_names, len(column_names)))[:random.randint(1, len(column_names))].upper()} FROM {table_name};",
        "select_columns": f"SELECT {', '.join(random.sample(column_names, random.randint(1, len(column_names))))} FROM {table_name};",
        "group_by": f"SELECT {pick_column(string_columns)}, COUNT(*) FROM {table_name} GROUP BY {pick_column(string_columns)};"
        if string_columns else "Not applicable for this dataset.",
        "group_by_having": f"SELECT {pick_column(string_columns)}, AVG({pick_column(int_float_columns)}) FROM {table_name} "
                           f"GROUP BY {pick_column(string_columns)} HAVING AVG({pick_column(int_float_columns)}) > {random.randint(10, 100)};"
        if string_columns and int_float_columns else "Not applicable for this dataset.",
        "where_clause": f"SELECT {pick_column(int_float_columns)}, {pick_column(string_columns)} FROM {table_name} WHERE {pick_column(int_float_columns)} > {random.randint(50, 500)};"
        if int_float_columns else "Not applicable for this dataset.",
        "order_by": f"SELECT {pick_column(int_float_columns)}, {pick_column(string_columns)} FROM {table_name} ORDER BY {pick_column(int_float_columns)} DESC;"
        if int_float_columns else "Not applicable for this dataset.",
        "limit": f"SELECT {pick_column(column_names)}, {pick_column(column_names)} FROM {table_name} LIMIT {random.randint(5, 20)};",
        "join": f"SELECT a.{pick_column(string_columns)}, b.{pick_column(string_columns)} FROM {table_name} a "
                f"JOIN {table_name}_related b ON a.{pick_column(string_columns)} = b.{pick_column(string_columns)};"
        if string_columns else "Not applicable for this dataset.",
        "average": f"SELECT AVG({pick_column(int_float_columns)}) AS average_{pick_column(int_float_columns)} FROM {table_name};"
        if int_float_columns else "Not applicable for this dataset.",
        "max": f"SELECT MAX({pick_column(int_float_columns)}) AS max_{pick_column(int_float_columns)} FROM {table_name};"
        if int_float_columns else "Not applicable for this dataset.",
        "min": f"SELECT MIN({pick_column(int_float_columns)}) AS min_{pick_column(int_float_columns)} FROM {table_name};"
        if int_float_columns else "Not applicable for this dataset."
    }

    # Ensure at least one query for each required type
    required_queries = {
        "select_all", "select_columns", "group_by", "group_by_having",
        "where_clause", "order_by", "limit", "join", "average", "max", "min"
    }

    # Filter out invalid queries
    valid_queries = {key: query for key, query in queries.items() if "Not applicable" not in query}

    # Check if any required query type is missing
    for query_type in required_queries:
        if query_type not in valid_queries:
            valid_queries[query_type] = f"-- Could not generate a valid {query_type} query for the dataset."

    return valid_queries

# Custom Function for the NL to SQL Conversion
def extract_col_name(file_name):
    # Get the file extension
    _, file_extension = os.path.splitext(file_name)
    
    try:
        if file_extension == ".csv":
            # CSV file
            data = pd.read_csv(file_name)
        elif file_extension in [".xls", ".xlsx"]:
            # Excel file
            data = pd.read_excel(file_name)
        elif file_extension == ".json":
            # JSON file
            data = pd.read_json(file_name)
        elif file_extension in [".txt", ".tsv"]:
            # Text file with delimiters (assume tab-separated by default)
            data = pd.read_csv(file_name, delimiter="\t")
        else:
            print(f"Unsupported file type: {file_extension}")
            return []
        
        # Return the column names as a list
        return [col.lower() for col in data.columns]
    
    except Exception as e:
        print(f"An error occurred while processing the file: {e}")
        return []

## Custom Function for Random SQL Query Generator
def extract_columns_and_types(file_name):
    try:
        df = pd.read_csv(file_name)

        column_names = df.columns.tolist()

        data_types = {}
        for col in column_names:
            if pd.api.types.is_integer_dtype(df[col]):
                data_types[col] = "INT"
            elif pd.api.types.is_float_dtype(df[col]):
                data_types[col] = "FLOAT"
            elif pd.api.types.is_string_dtype(df[col]):
                data_types[col] = "VARCHAR(255)"
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                data_types[col] = "DATETIME"
            else:
                data_types[col] = "TEXT"

        return column_names, data_types

    except Exception as e:
        print(f"Error processing file: {e}")
        return [], {}

def extract_file_name(file_path):
    # Get the file name without the extension
    file_name_without_extension = os.path.splitext(os.path.basename(file_path))[0]
    sanitized_file_name = re.sub(r'[^a-zA-Z0-9]', '', file_name_without_extension)
    return sanitized_file_name

# -- Stop Words
def remove_stopwords(phrase):
    stop_words = set(stopwords.words('english'))
        # Split the phrase into words and filter out stop words
    filtered_words = [word for word in phrase.split() if word.lower() not in stop_words]
    
    # Return the phrase without stop words
    return ' '.join(filtered_words)

# option 2 Server Tables
def get_table_columns(cursor, table_name):
    try:
        # Use the DESCRIBE command in MySQL to fetch column names and types
        cursor.execute(f"DESCRIBE {table_name};")
        columns_info = cursor.fetchall()

        # Extract column names and data types
        column_names = [column[0].lower() for column in columns_info]
        column_types = {column[0].lower(): column[1] for column in columns_info}

        return column_names, column_types

    except mysql.connector.Error as e:
        print(f"MySQL error: {e}")
        return [], {}

# ------------------------------------- SQL Skeleton
# Function to parse SELECT clause
# --------  NEW SELECT V2 

def parse_select(phrase, column_names, file_name):
    # Supported aggregate functions and their synonyms
    aggregates = {
        "max": ["max", "maximum"],
        "min": ["min", "minimum"],
        "sum": ["sum", "total"],
        "avg": ["avg", "average"],
        "count": ["count", "number"]
    }

    select_columns = []
    part_wo_sql = ""
    table_name = extract_file_name(file_name)
    # Iterate over each part of the phrase to find the relevant instruction
    for parts in phrase:
        if "show" in parts.lower() or "select" in parts.lower() or "give" in parts.lower():
            part_wo_sql = parts

    # Check if any other part of the phrase contains "JOIN"
    has_join = any("join" in parts.lower() for parts in phrase if parts != part_wo_sql)

    # Remove SQL keywords like "select" or "show"
    part_wo_sql = part_wo_sql.replace("select", "").replace("show", "")
    part_wo_sql = remove_stopwords(part_wo_sql)

    # Split the remaining text by commas to process individual items
    temp = part_wo_sql.split(',')

    for names in temp:
        name = names.lower().strip()
        matched_aggregate = None
        matched_column = None

        # Check for aggregate functions
        for func, synonyms in aggregates.items():
            if any(word in name for word in synonyms):
                matched_aggregate = func.upper()
                # Extract column name from the remaining part of the phrase
                column_part = name.replace(synonyms[0], "").replace(synonyms[-1], "").strip()
                for col in column_names:
                    if col.lower().startswith(column_part[:3]):  # Fuzzy matching
                        matched_column = col.upper()
                        break
                break

        # If an aggregate function is matched
        if matched_aggregate and matched_column:
            if has_join and table_name:
                select_columns.append(f"{table_name}.{matched_aggregate}({matched_column})")
            else:
                select_columns.append(f"{matched_aggregate}({matched_column})")
        # If it's a standard column name
        elif name in column_names:
            if has_join and table_name:
                select_columns.append(f"{table_name.upper()}.{name.upper()}")
            else:
                select_columns.append(name.upper())

    # If no columns are specified, default to SELECT *
    if not select_columns:
        return "SELECT *"
    
    return f"SELECT {', '.join(select_columns)}"

# --------  NEW SELECT V2
# Function to parse FROM clause
def parse_from(file_path):

    file_name = extract_file_name(file_path)
    return f"FROM {file_name}"

def parse_join(user_input_array, original_columns, original_table):

    join_phrase = ""
    # Find the part of the input array that contains "join"
    for section in user_input_array:
        if "join" in section.lower():
            join_phrase = section
            break

    print("JP: ", join_phrase)
    if not join_phrase:
        return ""

    # Split the join phrase into parts
    parts = join_phrase.split()

    # Ensure the phrase follows the expected structure
    if len(parts) >= 5 and "join" in parts[0].lower() and "on" in parts:
        table_name = parts[1]  # Table to join
        join_column = parts[2]  # Column from the joining table
        original_column = parts[4]  # Column from the original table

        # Validate the original column exists in the original table
        if original_column.lower() not in original_columns:
            return f"Error: Column '{original_column}' not found in the original table."

        # Construct the JOIN clause
        join_clause = f"JOIN {table_name} ON {original_table}.{original_column} = {table_name}.{join_column}"
        return join_clause

    return ""

# Function to parse WHERE clause (multiple conditions)
def parse_where(phrase, column_names):

    for parts in phrase:
        if "where" in parts.lower():
            part_wo_sql = parts.split("where", 1)[1].strip()
            condition = part_wo_sql

            condition = remove_stopwords(condition)
            print("condition",condition)
            # Replace words with symbols
            condition = (condition.replace("greater equal", ">=")
                                   .replace("less equal", "<=")
                                   .replace("greater", ">")
                                   .replace("lesser", "<")
                                   .replace("less", "<")
                                   .replace("equals", "=")
                                   .replace("equal", "=")
                                   .replace("not equal", "!="))

            # Check for logical operators
            logical_operators = {"and", "or"}
            condition_parts = condition.split()
            # condition_parts = [part.lstrip().strip() for part in condition_parts]
            condition_parsed = []
            current_condition = []

            for word in condition_parts:
                if word in logical_operators:
                    if current_condition:
                        condition_parsed.append(' '.join(current_condition))
                        condition_parsed.append(word.upper())
                        current_condition = []
                elif word.lower() in column_names or word.isdigit() or any(op in word for op in ["<", ">", "=", "!="]):
                    current_condition.append(word)
                else:
                    current_condition.append(f"'{word}'")
            
            if current_condition:
                condition_parsed.append(' '.join(current_condition))

            return f"WHERE {' '.join(condition_parsed)}"
    return ""

# Function to parse GROUP BY clause
def parse_group_by(phrase, column_names):
    for parts in phrase:
        if "group" in parts.lower():
            group_by_part = parts.split("group", 1)[1].strip()
            group_by_columns = [col for col in group_by_part.split() if col in column_names]
            if group_by_columns:
                return f"GROUP BY {', '.join(group_by_columns)}"
    return ""

# Function to parse HAVING clause (multiple conditions)
def parse_having(phrase, column_names):

    for parts in phrase:
        if "having" in parts.lower():
            part_wo_sql = parts.split("having", 1)[1].strip()
            condition = part_wo_sql

            condition = remove_stopwords(condition)
            # print("condition",condition)
            # Replace words with symbols
            condition = (condition.replace("greater equal", ">=")
                                   .replace("less equal", "<=")
                                   .replace("greater", ">")
                                   .replace("less", "<")
                                   .replace("equal", "=")
                                   .replace("not equal", "!="))

            # Check for logical operators
            logical_operators = {"and", "or"}
            condition_parts = condition.split()
            condition_parsed = []
            current_condition = []

            for word in condition_parts:
                if word.lower() in logical_operators:
                    if current_condition:
                        condition_parsed.append(' '.join(current_condition))
                        condition_parsed.append(word.upper())
                        current_condition = []
                elif word in column_names or word.isdigit() or any(op in word for op in ["<", ">", "=", "!="]):
                    current_condition.append(word)
                else:
                    current_condition.append(f"'{word}'")
            
            if current_condition:
                condition_parsed.append(' '.join(current_condition))

            return f"HAVING {' '.join(condition_parsed)}"
    return ""

# Function to parse ORDER BY clause
def parse_order_by(phrase, column_names):
    for parts in phrase:
        if "order" in parts.lower():
            order_by_part = parts.split("order", 1)[1].strip()
            order_by_columns = [] 
            # order_by_columns = [word.upper() if word.lower() in ["asc", "desc", "descending", "ascending"] else word for word in order_by_part.split() if word in column_names or word.lower() in ["asc", "desc"]]
        # Split the order by part and find matching columns
            for word in order_by_part.split():
                if word in column_names or word.lower() in ["asc", "desc", "descending", "ascending"]:
                    if word == "ascending":
                        word = "asc"
                    elif word == "descending":
                        word = "desc"
                    
                    order_by_columns.append(word)

            if order_by_columns:
                return f"ORDER BY {' '.join(order_by_columns)}"
    return ""

def parse_limit(phrase):
    for part in phrase:
        # Check if the phrase contains the word "limit"
        if "limit" in part.lower():
            # Split the part into words
            words = part.split()
            words = remove_stopwords(words)
            # Find the word "limit" and check what follows
            for i, word in enumerate(words):
                if word.lower() == "limit":
                    # Check for a number after "limit" or "to"
                    if i + 1 < len(words) and words[i + 1].isdigit():
                        return f"LIMIT {words[i + 1]}"
                    elif i + 2 < len(words) and words[i + 1].lower() == "to" and words[i + 2].isdigit():
                        return f"LIMIT {words[i + 2]}"

    return ""


# -------------- AUXILIARY FUNCTIONS
# Function to split the sentence into parts based on SQL keywords
def separate_sentence_parts(sentence):
    sql_keywords = {"SELECT", "GIVE", "SHOW", "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "JOIN", "LIMIT"}
    
    # Split the sentence into words
    words = sentence.split()
    
    # Initialize an empty list to store sentence parts
    sentence_parts = []
    current_part = []
    
    # Iterate through the words, building sentence parts based on SQL keywords
    for word in words:
        if word.upper() in sql_keywords:
            if current_part:
                sentence_parts.append(' '.join(current_part))
                current_part = []
        current_part.append(word)
    
    # Add the last part to the list
    if current_part:
        sentence_parts.append(' '.join(current_part))
    
    return sentence_parts

def select_existing_table(cursor, connection):

    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
            
    if tables:
        print("Existing tables in the database:")
        for table in tables:
                print(f"- {table[0]}")
    else:
        print("This database has no tables currently.")
    return tables

def upload_file(filepath, cursor, connection):

    table_name = extract_file_name(filepath)            
        # Check for existing tables in the database
        # cursor = connection.cursor()
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
    column_names, column_types = extract_columns_and_types(filepath)

    column_definitions = ", ".join(
        [f"`{col}` {dtype}" for col, dtype in column_types.items()]
    )
            
            # Proceed to upload the CSV file
    print(f"\nPreparing to upload the file to table '{table_name}'...")
            
            # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv(filepath)
    # print(f"Read CSV file: {filepath}")
            
             # Create the table if it doesn't exist
    if table_name not in tables:
        print(f"Table '{table_name}' does not exist. Creating table...")
        # column_definitions = ", ".join(
        #         [f"`{col}` VARCHAR(255)" for col in df.columns]
        #     )  # Define all columns as VARCHAR(255) for simplicity
        
        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({column_definitions});"

        # create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({column_definitions});"
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created successfully.")

    else:                
            # Remove existing data from the table
        print(f"Clearing existing data from table '{table_name}'...")
        delete_query = f"DELETE FROM {table_name};"
        cursor.execute(delete_query)
        connection.commit()
        print(f"Existing data removed from table '{table_name}'.")
            
            # Insert data into the database
        print("Uploading new data...")
            
            # Generate the SQL insert statement
    columns = ", ".join(df.columns)  # Extract column names
    placeholders = ", ".join(["%s"] * len(df.columns))  # Create placeholders for values
    insert_query = f"INSERT INTO `{table_name}` VALUES ({placeholders})"

            # Convert DataFrame rows to a list of tuples
    data = [tuple(row) for row in df.to_numpy()]
           
            # Execute insert query
    cursor.executemany(insert_query, data)
    connection.commit()
            
    print(f"Successfully uploaded {cursor.rowcount} rows into table '{table_name}'.")

def english_to_sql(user_input, file_path, column_names):

    sql_query_parts = []
    # print(user_input)
    try:

        broken_input = separate_sentence_parts(user_input)
        sql_query_parts.append(parse_select(broken_input, column_names, file_path).upper())
        sql_query_parts.append(parse_from(file_path).upper())
        sql_query_parts.append(parse_join(broken_input, column_names, file_path))
        sql_query_parts.append(parse_where(broken_input, column_names).upper())
        sql_query_parts.append(parse_group_by(broken_input, column_names).upper())
        sql_query_parts.append(parse_having(broken_input, column_names).upper())
        sql_query_parts.append(parse_order_by(broken_input, column_names).upper())
        sql_query_parts.append(parse_limit(broken_input))

        sql_query = " ".join([part for part in sql_query_parts if part])
    
        return sql_query.strip() + ";"
    except:
        print("Could not Understand query please, try again")
        

user_choice = 0

# ----------------------- MAIN DRIVER FUNCTION 
def sql_workflow_manager():

    database = "dsci551gp"    
    try:
        # Connect to the SQL Server
        connection = mysql.connector.connect(
            host="localhost",
            database=database,
            user="root",    
            password="mysqlpassword#1"
        )
        if connection == None:
            print("-"*50)
            print("SQL Server Not found")
            print("-"*50)
            return

        if connection.is_connected():
            cursor = connection.cursor()
            print(f"Connected to database '{database}' successfully!")
            database_selected = False
            while True:
                print("-"*50)
                print("Do you want to work on a new data table or existing ones?")
                print("1. New Data Table")
                print("2. Existing Data Table")
                print("3. Exit")
        
                choice = input("Enter your choice: ")

                if choice == "1":  # New Data Table
                    filename = input("Enter the file name to upload: ")
                    user_choice = 1

                    try:
                        upload_file(filename, cursor, connection=connection)
                        table_name = extract_file_name(filename) 
                    # --------- SQL Random Query Generator
                        happy_w_generation = False
                        while happy_w_generation == False:     
                            rand_queries = generate_sql_queries(filename)
                            print("-"*23, "START AUTO Generated SQL QUERIES", "-"*23)
                            print("Here are some example queries that you can run on your table")
                            print("------------------------------------------------------------")
                            is_valid = True
                            for query_name, query in rand_queries.items():
                                print(f"{query_name}:\n{query}\n")        

                            while True:  # Loop until the user provides a valid response
                                regen = input("Do you want me to regenerate queries? (type yes/no): ").strip().lower()
                                if regen == "yes":
                                    break
                                elif regen == "no":
                                    print("-"*23, "END OF Auto Generated SQL QUERIES", "-"*23)
                                    database_selected = True
                                    happy_w_generation = True
                                    break
                                else:
                                    print("Unrecognized input. Please type 'yes' or 'no'.")
                    # ---------- SQL Random Query Generator
                    except:
                        print("This File Does not exist, Please Try again")
        
                elif choice == "2":  # Existing Data Table
                    existing_tables = select_existing_table(cursor, connection)
                    user_choice = 2
                    if existing_tables:
                        print("Available tables:")
                        for i, table in enumerate(existing_tables, start=1):
                            print(f"{i}. {table[0]}")
        
                         # Ask the user to select a table
                        try:
                            table_choice = int(input("Please enter the number of the table you want to select: "))
                            if 1 <= table_choice <= len(existing_tables):
                                selected_table = existing_tables[table_choice - 1][0]
                                print(f"You have selected the table: {selected_table}")
                #  Optionally, you can now do something with the selected table, like querying it.
                                tablename = selected_table
                                filename = selected_table
                    # --------- SQL Random Query Generator
                                happy_w_generation = False
                                while happy_w_generation == False:     
                                    # print("All GOOD")      
                                    print("-"*23, "Auto Generated SQL QUERIES", "-"*23)
                                    rand_queries = generate_Serversql_queries(cursor, tablename)
                                    print("Here are some example queries that you can run on your table")
                                    print("------------------------------------------------------------")
                                    is_valid = True
                                    for query_name, query in rand_queries.items():
                                        print(f"{query_name}:\n{query}\n")        

                                    while True:  # Loop until the user provides a valid response
                                        regen = input("Do you want me to regenerate queries? (type yes/no): ").strip().lower()
                                        if regen == "yes":
                                            break
                                        elif regen == "no":
                                            print("-"*23, "END OF Auto Generated SQL QUERIES", "-"*23)
                                            database_selected = True
                                            happy_w_generation = True
                                            break
                                        else:
                                            print("Unrecognized input. Please type 'yes' or 'no'.")
                    # ---------- SQL Random Query Generator

                            else:   
                                print("Invalid selection, please choose a valid number.")
                        
                        except ValueError:
                            database_selected = False
                            print("Invalid input. Please enter a number corresponding to a table.")
                    else:
                        database_selected = False
                        print("No tables found in the database.")
            
                elif choice == "3":  # Exit
                    print("Exiting the program.")
                    break
        
                else:
                    database_selected = False
                    print("Invalid choice. Please try again.")
        
        # Database Selection is Over
        # User Choice Selector
                while database_selected == True:
                    print("Do you want to:")
                    print("1. Write in English")
                    print("2. Work directly with SQL statements")
                    print(f"3. See {filename} preview")
                    print(f"4. Describe {filename}")
                    print("5. Go back")
                
                    sub_choice = input("Enter your choice: ")

                 
                    if sub_choice == "1":  # English
                        if user_choice == 1:
                            # Local Extractor
                            column_names = extract_col_name(file_name=filename)
                        else:
                                # Server Extractor
                            column_names, column_types = get_table_columns(cursor, table_name=tablename)

                        print("-"*23, "Table Information", "-"*23)
                        print("Existing Tables: \n", existing_tables)
                        print("Available Col Names & Data Types \n", column_types)
                        print("-"*23, "Table Information", "-"*23)


                        print("-"*23, "GUIDELINES", "-"*23)
                        print("1. Seperate Column names by a comma")
                        print("2. While attempting JOIN Statment: use JOIN <Tablename> <New Table columnName> on <OLD TABLE Col Name>")
                        print("3. Make sure the spelling of the col names are correct, case insensitive")
                        print("4 Do not add unecessary special characters")
                        print("-"*23, "END OF GUIDELINES", "-"*23, "\n")

                        english_query = input("Enter your query in English (or Type 'exit'):  ")

                        if english_query.lower() == "exit":
                            continue

                        sql_query = english_to_sql(english_query, filename, column_names)


                        while True:
                            print("-"*30)
                            print(f"Generated SQL: {sql_query}")
                            happy = input("Are you happy with the SQL code? (yes/no/back): ").strip().lower()
                            print("-"*30)
                            if happy == "yes":
                                execute_Sql_squery(cursor, sql_query)
                                break
                            elif happy == "no":
                                english_query = input("Enter your query in English again: ")
                                sql_query = english_to_sql(english_query, filename, column_names)
                            elif happy == "back":
                                break
                            else:
                                print("Invalid input. Please enter 'yes', 'no', or 'back'.")
                    
                    elif sub_choice == "2":  # SQL Statements
                        # print out useful information
                        
                        if user_choice == 1:
                            # Local Extractor
                            column_names = extract_col_name(file_name=filename)
                        else:
                            # Server Extractor
                            column_names, column_types = get_table_columns(cursor, table_name=tablename) 

                        print("-"*23, "Table Information", "-"*23)
                        print("SELECTED TABLE: ", filename)
                        print("Existing Tables: \n", existing_tables)
                        print("Available Col Names & Data Types \n", column_types)
                        print("-"*23, "Table Information", "-"*23)

                        print("-"*23, "GUIDELINES", "-"*23)
                        print("1. Make sure the syntax is correct")
                        print("2. Do not forget to add ';' at the end")
                        print("-"*23, "END OF GUIDELINES", "-"*23)

                        sql_query = input("Enter your SQL statement: ")
                        execute_Sql_squery(cursor, sql_query)
                    
                    elif sub_choice == "3":  # Go back
                        # print("TNaem", filename)
                        table_name = extract_file_name(filename) 
                        execute_Sql_squery(cursor, f"Select * from {table_name} limit 5;")

                    elif sub_choice == "4":
                        print("-"*23, "Table Description", "-"*23)
                        table_name = extract_file_name(filename)
                        execute_Sql_squery(cursor, f"desc {table_name}")
                        print("-"*23, "End of Table Description", "-"*23)
                
                    elif sub_choice == "5":
                        break
                    
                    else:
                        print("Invalid choice. Please try again.")



#  ---------- ERROR HANDLING -------------
    except ValueError as e:
        print("-"*50)
        print("SQL Server Not found")
        print("-"*50)

    except AttributeError as e:
        print("-"*50, "Error Message", "-"*50)
        print("Connection object does not have 'is_connected' method.")
        print(f"Error: {e}")
        print("-"*50, "Error Message", "-"*50)

    except mysql.connector.InterfaceError as e:
    # Handle database interface errors (e.g., connection lost or invalid)
        print("-"*50, "Error Message", "-"*50)        
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {e}")
        print("-"*50, "Error Message", "-"*50)


    except Error as e:
            print("-"*50, "Error Message", "-"*50)
            print(f"Error Type: {type(e).__name__}")
            print("Unable to Connect to Server, Please Try Again")
            print(f"Error: {e}")
            print("-"*50, "Error Message", "-"*50)
            

# ---------- Closing the Connection to SQL Server -----------
    finally:
        # Close the connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("-"*40, "GoodBye", "-"*40)
            print("Connection closed.")
            print("-"*40, "GoodBye", "-"*40)
# sql_workflow_manager()