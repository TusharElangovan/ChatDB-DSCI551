import os
import json
import pandas as pd
import pymongo
from pymongo import MongoClient, errors
import spacy
from spacy.tokenizer import Tokenizer
from spacy.util import compile_infix_regex
from bson import json_util
import random

nlp = spacy.load("en_core_web_sm")

def custom_tokenizer(nlp):
    infixes = nlp.Defaults.infixes + [r"(?<=[\w])_(?=[\w])"]  # maintain the underscore for tokens like _id
    infix_re = compile_infix_regex(infixes)
    return Tokenizer(nlp.vocab, infix_finditer=infix_re.finditer)

nlp.tokenizer = custom_tokenizer(nlp)

keywords = {
    "query_words": [
        "show", "find", "give", "display", "list", "fetch", "reveal", "return", # Projecting
        "present", "extract", "output", "include", "select", 
        "where", "filter", "with", # Filtering
        "sort", "order", "rank", "ordered", "sorted", "ranked", # Sorting
        "limit", "top", "first",  # Limiting
        "skip", "offset",  # Skipping
        "group", "aggregate", # Aggregation
        "distinct", "unique" # Distinct
    ],
    "comparison": [
        "greater", "less", "equal", "greater equal", "less equal", "greater than or equal to", "less than or equal to", "more than", "less than",
        "more than or equal to", "fewer than", "fewer than or equal to", "equals", "=", ">", "<", "<=", ">=", "gt", "gte", "lt", "lte", "eq","ne", "matches"
    ],
    "logical": [
        "and", "or", "nor", "as", "but", "either", "neither"
    ],
    "text_matching": [
        "like", "starts", "ends", "regex", "not"
    ],
    "existence": [
        "exists", "has", "lacks"
    ],
    "sort_order": [
        "ascending", "asc", "descending", "desc"
    ],
    "agg_functions": [
        "sum", "avg", "average", "count", "min", "max", "minimum", "maximum", "group", "aggregate"
    ]
}

stop_words = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", 
    "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", 
    "can", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", 
    "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", 
    "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", 
    "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", 
    "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", 
    "no", "of", "off", "on", "once", "other", "ought", "our", "ours", "to",
    "ourselves", "out", "over", "own", "she", "she'd", "she'll", "she's", "should", "shouldn't", 
    "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", 
    "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", 
    "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", 
    "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", 
    "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", 
    "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
}

try:
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
    client.server_info() 
except errors.ServerSelectionTimeoutError:
    print("Error: Could not connect to the MongoDB server. Ensure that MongoDB is running locally.")
    exit(1)

# Database and collection functions
def show_databases():
    try:
        databases = client.list_database_names()
        print("\nAvailable Databases:")
        for db in databases:
            print(f" - {db}")
        return databases
    except errors.PyMongoError as e:
        print(f"Error retrieving databases: {e}")
        return []

def show_collections(db_name):
    try:
        db = client[db_name]
        collections = db.list_collection_names()
        print(f"\nCollections in '{db_name}':")
        for col in collections:
            print(f" - {col}")
        return collections
    except errors.PyMongoError as e:
        print(f"Error retrieving collections from database '{db_name}': {e}")
        return []

def upload_data_to_db(db_name, collection_name, data):
    try:
        db = client[db_name]
        collection = db[collection_name]
        collection.insert_many(data)
        print(f"\nData successfully uploaded to '{db_name}.{collection_name}'.")
    except errors.PyMongoError as e:
        print(f"Error uploading data to '{db_name}.{collection_name}': {e}")

def convert_json_to_dict(json_file):
    try:
        if not os.path.exists(json_file):
            raise FileNotFoundError(f"JSON file '{json_file}' not found.")
        with open(json_file, 'r') as file:
            data = json.load(file)
            if isinstance(data, dict): 
                return [data]
            elif isinstance(data, list): 
                return data
            else:
                raise ValueError(f"Invalid JSON structure in '{json_file}'. Expected list or dict.")
    except FileNotFoundError as e:
        print(e)
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON file '{json_file}': {e}")
        return None
    except ValueError as e:
        print(e)
        return None

def convert_csv_to_json(csv_file):
    try:
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"CSV file '{csv_file}' not found.")
        df = pd.read_csv(csv_file)
        return json.loads(df.to_json(orient="records"))
    except FileNotFoundError as e:
        print(e)
        return None
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file '{csv_file}': {e}")
        return None

def get_available_fields(collection):
    document = collection.find_one() 
    field_types = {k: type(v).__name__ for k, v in document.items()} if document else {}
    available_fields = list(document.keys()) if document else []
    available_fields.extend(["all", "collections", "data"])  # Add default options for "all fields"
    
    return available_fields, field_types

def tokenize_and_clean(user_query, available_fields):
    doc = nlp(user_query)
    tokens = {
        "query_sections": [],  # List of query sections defined by query words
        "comparison": [],  # List of comparison operators
        "values": [],  # List of standalone values
        "logical": [],  # Logical operators for filtering
        "sort": [],  # Sorting instructions: (field, order)
        "limit": None,  # Single numeric value for limit
        "skip": None,  # Single numeric value for skip
        "aggregate": None  # Indicates if an aggregation query is detected
    }
    current_section = {"query_word": None, "fields": [], "order": []}  # Tracking query word section

    multi_word_comparisons = {
        "greater than or equal to": "greater equal",
        "less than or equal to": "less equal",
        "greater than": "greater",
        "less than": "less",
        "at least": "greater equal",
        "at most": "less equal",
        ">=": "greater equal",
        "<=": "less equal",
        "=": "equal",
        "equals": "equal",
        "not equal": "not equal",
        ">": "greater",
        "<": "less",
        "not": "not equal",
        "equal": "equal",
        "gt": "greater",
        "lt": "less",
        "gte": "greater equal",
        "lte": "less equal",
        "ne": "not equal",
        "eq": "",
        "matches": ""
    }

    i = 0
    while i < len(doc):
        token = doc[i]
        text = token.text

        if text in keywords["query_words"]:  # Start a new section for query words
            if text in ["group", "aggregate"]:  # Detect aggregation
                tokens["aggregate"] = 1
                if current_section["query_word"]:  # Save the previous section
                    tokens["query_sections"].append(current_section)
                current_section = {"query_word": "agg", "fields": [], "order": []}
            elif text in ["limit", "top", "first"]:  # Detect limit
                if i + 1 < len(doc) and doc[i + 1].like_num:
                    tokens["limit"] = int(doc[i + 1].text)
                    i += 1
            elif text in ["skip", "offset"]: 
                if i + 1 < len(doc) and doc[i + 1].like_num:
                    tokens["skip"] = int(doc[i + 1].text)
                    i += 1
            else:
                if current_section["query_word"]:  # Save the previous section
                    tokens["query_sections"].append(current_section)
                current_section = {"query_word": text, "fields": [], "order": []}
        elif text in available_fields:  # Only include valid field names
            current_section["fields"].append(text.strip())
            if current_section["query_word"] in ["show", "find", "give", "display", "list", "fetch", "reveal", "return", "present", "extract", "output", "include", "select"]:
                if len(current_section["order"]) < len(current_section["fields"]):
                    current_section["order"].append("group")
        elif text in keywords["agg_functions"]:  # Detect aggregation functions
            if current_section["query_word"] in ["show", "find", "give", "display", "list", "fetch", "reveal", "return", "present", "extract", "output", "include", "select"]:
                current_section["order"].append(text) 
        elif text in ["asc", "ascending", "desc", "descending"]: 
            normalized_order = 1 if text in ["asc", "ascending"] else -1
            current_section["order"].append(normalized_order)
        elif text in keywords["logical"]: 
            if current_section["query_word"] in ["where", "filter", "with"]:
                tokens["logical"].append(text)
        elif token.like_num: 
            tokens["values"].append(int(text))
        else:
            found_operator = None
            for phrase, simplified_operator in multi_word_comparisons.items():
                phrase_tokens = phrase.split()
                if (
                    len(phrase_tokens) <= len(doc) - i
                    and all(doc[i + j].text == phrase_tokens[j] for j in range(len(phrase_tokens)))
                ):
                    found_operator = simplified_operator
                    i += len(phrase_tokens) - 1
                    break

            if found_operator:
                tokens["comparison"].append(found_operator)
            elif text not in stop_words:  # Add as value if not a stop word
                tokens["values"].append(text)
        i += 1

    if current_section["query_word"]:
        tokens["query_sections"].append(current_section)

    for section in tokens["query_sections"]:
        if section["query_word"] in ["where", "filter", "with"]:
            fields = section["fields"]
            aligned_comparisons = []
            aligned_values = []

            for i, field in enumerate(fields):
                if i < len(tokens["comparison"]):
                    operator = tokens["comparison"][i]
                else:
                    operator = "equal"  # Default to equal if no operator

                if i < len(tokens["values"]):
                    value = tokens["values"][i]
                else:
                    value = None  # Default to None if no value
                aligned_comparisons.append(operator)
                aligned_values.append(value)

            tokens["comparison"] = aligned_comparisons
            tokens["values"] = aligned_values

        elif section["query_word"] in ["sort", "order", "rank", "ordered", "sorted", "ranked"]:
            fields = section["fields"]
            orders = section["order"]

            while len(orders) < len(fields):
                orders.append(1)  # Default ascending order for remaining fields

            orders = orders[:len(fields)]
            section["order"] = orders  # Update order

    return tokens

def parse_query(user_query, available_fields, field_types):
    """
    Parses the user query to generate MongoDB query, projection, aggregation pipeline,
    and additional parameters (sorting, limit, skip).
    """
    categorized_tokens = tokenize_and_clean(user_query, available_fields)
    # print(f"Processed tokens: {categorized_tokens}") # this is for testing purposes for tokenization 

    query = {}
    projection = {}
    explicitly_include_id = False
    filter_conditions = []  # Store conditions for logical operators
    sorting_conditions = []  # Store sorting conditions
    limit = None # Store limit value
    skip = None # Store skip value
    aggregation_pipeline = None # Determine if aggregate function

    for section in categorized_tokens["query_sections"]:
        query_word = section["query_word"]
        fields = section.get("fields", [])

        if query_word in ["show", "select", "give", "find", "return"]:  # Projection
            if not fields or any(field in ["all", "collections", "data"] for field in fields):
                projection = None  # Show all fields by default
            else:
                for field in fields:
                    if field in available_fields:
                        projection[field] = 1
                    if field == "_id":
                        explicitly_include_id = True

        elif query_word in ["where", "filter", "with", ""]:  
            values = categorized_tokens["values"][:]
            comparisons = categorized_tokens["comparison"][:]
            for i, field in enumerate(fields):
                if field in available_fields:
                    operator = comparisons.pop(0) if comparisons else "equal"
                    value = values.pop(0) if values else None
                    # Data typing
                    if field in field_types:
                        expected_type = field_types[field]
                        try:
                            if expected_type == "int":
                                value = int(value)
                            elif expected_type == "float":
                                value = float(value)
                            elif expected_type == "string":
                                value = str(value)
                        except ValueError:
                            raise ValueError(f"Invalid value '{value}' for field '{field}' of type '{expected_type}'.")

                    # Map to MongoDB
                    if operator in ["greater", "greater than", "more than", ">", "gt"]:
                        condition = {field: {"$gt": value}}
                    elif operator in ["greater equal", "greater than or equal to", "at least", ">=", "gte"]:
                        condition = {field: {"$gte": value}}
                    elif operator in ["less", "less than", "fewer than", "<", "lt"]:
                        condition = {field: {"$lt": value}}
                    elif operator in ["less equal", "less than or equal to", "at most", "<=", "lte"]:
                        condition = {field: {"$lte": value}}
                    elif operator in ["not equal", "does not equal", "!=", "not", "ne"]:
                        condition = {field: {"$ne": value}}
                    elif operator in ["equal", "equals", "=", "eq"]:
                        condition = {field: {"$eq": value}}
                    else:
                        raise ValueError(f"Unknown operator: {operator}")

                    filter_conditions.append(condition)

        elif query_word in ["sort", "order", "rank", "ordered", "sorted", "ranked"]: 
            orders = section.get("order", [])
            for i, field in enumerate(fields):
                if field in available_fields:
                    if i < len(orders):
                        order = orders[i]
                    else:
                        order = 1  # Default to ascending
                    sorting_conditions.append((field, order))

        elif query_word == "agg":  # Aggregation
            aggregation_pipeline = [] 
            group_stage = {"_id": {}}  # Grouping stage
            project_stage = {}  # Projection stage after grouping
            limit = categorized_tokens.get("limit")
            skip = categorized_tokens.get("skip")                        
    
            # Use fields and functions from "show" for aggregation
            show_section = next((s for s in categorized_tokens["query_sections"] if s["query_word"] in ["show", "select", "give", "find", "return"]), None)
            if show_section:
                show_fields = show_section["fields"]
                show_order = show_section["order"]  # Aggregation functions

                for i, field in enumerate(show_fields):
                    if field in available_fields:
                        agg_function = show_order[i] if i < len(show_order) else "group"

                        if agg_function == "group":
                            group_stage["_id"][field] = f"${field}"
                        else:
                            # Map function to MongoDB operator
                            mongo_operator = {
                                "sum": "$sum",
                                "avg": "$avg",
                                "count": {"$sum": 1},
                                "min": "$min",
                                "max": "$max",
                                "average": "$avg",
                                "minimum": "$min",
                                "maximum": "$max",
                            }.get(agg_function, None)

                            if mongo_operator is None:
                                raise ValueError(f"Unsupported aggregation function: {agg_function}")

                            if agg_function == "count":
                                group_stage[field] = mongo_operator
                            else:
                                group_stage[field] = {mongo_operator: f"${field}"}

                            # Include in projection
                            project_stage[field] = 1

            # Use fields from "agg" for grouping
            for field in fields:
                if field in available_fields:
                    group_stage["_id"][field] = f"${field}"  # Group by these fields
            aggregation_pipeline.append({"$group": group_stage})

            if query_word in ["sort", "order", "rank", "ordered", "sorted", "ranked"]: 
                orders = section.get("order", [])
                for i, field in enumerate(fields):
                    if field in available_fields:
                        if i < len(orders):
                            order = orders[i]
                        else:
                            order = 1  # Default to ascending
                        sorting_conditions.append((field, order))
 
            if sorting_conditions:
                sort_stage = {field: order for field, order in sorting_conditions if field in available_fields}
                if sort_stage:
                    aggregation_pipeline.append({"$sort": sort_stage})

            if project_stage:
                aggregation_pipeline.append({"$project": project_stage})

            if limit is not None:
                if isinstance(limit, int) and limit > 0:
                    aggregation_pipeline.append({"$limit": limit})
                else:
                    raise ValueError("Limit must be a positive integer.")

            if skip is not None:
                if isinstance(skip, int) and skip >= 0:
                    aggregation_pipeline.append({"$skip": skip})
                else:
                    raise ValueError("Skip must be a non-negative integer.")
    
    if aggregation_pipeline:
        if filter_conditions:
            # Use logical operators explicitly if they exist
            if "or" in categorized_tokens["logical"]:
                aggregation_pipeline.insert(0, {"$match": {"$or": filter_conditions}})
            elif "and" in categorized_tokens["logical"]:
                aggregation_pipeline.insert(0, {"$match": {"$and": filter_conditions}})
            else:
                # Default to $and if multiple conditions exist but no logical operator is specified
                if len(filter_conditions) > 1:
                    aggregation_pipeline.insert(0, {"$match": {"$and": filter_conditions}})
                elif len(filter_conditions) == 1:
                    aggregation_pipeline.insert(0, {"$match": filter_conditions[0]})
    else:
        if "or" in categorized_tokens["logical"]:
            query["$or"] = filter_conditions
        elif "and" in categorized_tokens["logical"]:
            query["$and"] = filter_conditions
        else:
            if len(filter_conditions) == 1:
                query.update(filter_conditions[0])
            elif len(filter_conditions) > 1:
                query["$and"] = filter_conditions
    # set limit/skip for normal pipeline            
    limit = categorized_tokens.get("limit")
    skip = categorized_tokens.get("skip")
    
    # Default to projecting all fields if no projection
    if not projection:
        projection = None
    elif "_id" not in projection and not explicitly_include_id:
        projection["_id"] = 0  # Exclude _id unless explicitly included

    return query, projection, sorting_conditions, limit, skip, aggregation_pipeline

def execute_query(collection, query, projection=None, sort_order=None, limit=None, skip=None, aggregation_pipeline=None):
    try:
        if aggregation_pipeline:
            pipeline_str = json.dumps(aggregation_pipeline, separators=(",", ":")) 
            query_str = f"db.{collection.name}.aggregate({pipeline_str})"
            print(f"MongoDB Code: {query_str}")

        else:
            projection_str = (
                "" if projection is None else f", {json.dumps(projection, separators=(',', ':'))}"
            )
            sort_order_str = (
                f".sort({json.dumps(sort_order, separators=(',', ':'))})" if sort_order else ""
            )
            skip_str = f".skip({skip})" if skip is not None else ""
            limit_str = f".limit({limit})" if limit is not None else ""
            query_str = f"db.{collection.name}.find({json.dumps(query, separators=(',', ':'))}{projection_str}){sort_order_str}{skip_str}{limit_str}"

            print(f"MongoDB Code: {query_str}")

        # Confirm correct before execute
        confirm = input("\nDoes this query look good? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("Query execution aborted. Please refine your input.")
            return

        # Execute the query or aggregation pipeline depending on user input
        if aggregation_pipeline:
            cursor = collection.aggregate(aggregation_pipeline)
        else:
            cursor = collection.find(query, projection)

            # Apply sorting
            if sort_order:
                if isinstance(sort_order, list) and all(isinstance(item, tuple) for item in sort_order):
                    cursor = cursor.sort(sort_order)
                else:
                    raise ValueError(
                        "Sort order must be a list of tuples (e.g., [('field', 1), ('field2', -1)])."
                    )

            # Apply skip
            if skip is not None:
                if isinstance(skip, int) and skip >= 0:
                    cursor = cursor.skip(skip)
                else:
                    raise ValueError("Skip must be a non-negative integer.")

            # Apply limit
            if limit is not None:
                if isinstance(limit, int) and limit > 0:
                    cursor = cursor.limit(limit)
                else:
                    raise ValueError("Limit must be a positive integer.")

        results = list(cursor)
        if results:
            print("\nQuery Results:")
            for idx, doc in enumerate(results, start=1):
                print(f"{idx}: {json.dumps(doc, indent=2, default=str)}")
        else:
            print("Query executed successfully, but no results were found.")

    except ValueError as ve:
        print(f"Invalid input: {ve}")
    except errors.PyMongoError as e:
        print(f"Error executing the query: {e}")

def generate_random_query(available_fields, field_types):
    valid_fields = [field for field in available_fields if field not in ['all', 'collections', 'data']]
    query_word = random.choice(["show", "find", "select"])
    num_fields = random.randint(1, min(3, len(valid_fields)))
    valid_group_by_fields = [field for field in available_fields if field not in ['all', 'collections', 'data', "_id"]]

    # Randomly decide if grouping is applied
    group_by_field = random.choice(valid_group_by_fields) if random.choice([True, False]) and len(valid_fields) > 1 else None

    # Ensure the grouped-by field is included in the selection
    if group_by_field:
        selected_fields = random.sample([field for field in valid_group_by_fields if field != group_by_field], num_fields - 1)
        selected_fields.insert(0, group_by_field)
    else:
        selected_fields = random.sample(valid_fields, num_fields)

    # Generate projection with aggregations if grouping
    projection = []
    for field in selected_fields:
        if group_by_field and field != group_by_field:
            agg_function = random.choice(["sum", "avg", "count", "max", "min"])
            projection.append(f"{agg_function} {field}")
        else:
            projection.append(field)

    projection_clause = " ".join(projection)

    # Randomly decide on a filter
    where_clause = ""
    if random.choice([True, False]):
        filter_field = random.choice(valid_fields)
        filter_type = field_types[filter_field]

        if filter_type in ["int", "Int64"]:
            operator = random.choice(["gt", "gte", "lt", "lte", "eq", "ne"])
            filter_condition = f"{filter_field} {operator} {random.randint(0, 100)}"
        elif filter_type in ["string", "str"]:
            operator = random.choice(["eq", "ne"])
            filter_condition = f"{filter_field} {operator} 'example{random.randint(1, 5)}'"
        elif filter_type == "float":
            operator = random.choice(["gt", "gte", "lt", "lte", "eq", "ne"])
            filter_condition = f"{filter_field} {operator} {random.uniform(0, 100):.2f}"
        else:
            filter_condition = None

        where_clause = f" where {filter_condition}" if filter_condition else ""

    # Randomly decide on sorting
    sort_clause = ""
    if random.choice([True, False]):
        sort_field = random.choice(valid_fields)
        sort_order = random.choice(["asc", "desc"])
        sort_clause = f" sort by {sort_field} {sort_order}"

    # Randomly add limit and skip
    limit_clause = f" limit {random.randint(1, 10)}" if random.choice([True, False]) else ""
    skip_clause = f" skip {random.randint(1, 5)}" if random.choice([True, False]) else ""

    # Add group by clause if applicable
    group_by_clause = f" group by {group_by_field}" if group_by_field else ""

    # Construct the final query
    random_query = f"{query_word} {projection_clause}{where_clause}{sort_clause}{group_by_clause}{limit_clause}{skip_clause}"
    return random_query

def execute_direct_query(client):

    databases = show_databases()
    user_db_choice = input("\nEnter a database name from the list above: ").strip()
    if user_db_choice.lower() == "exit":
        return
    if user_db_choice not in databases:
        print(f"Database '{user_db_choice}' not found.")
        return

    collections = show_collections(user_db_choice)
    user_col_choice = input("\nEnter a collection name from the list above: ").strip()
    if user_col_choice.lower() == "exit":
        return
    if user_col_choice not in collections:
        print(f"Collection '{user_col_choice}' not found.")
        return

    db = client[user_db_choice]
    collection = db[user_col_choice]

    print("\nYou can now enter a MongoDB query in JSON format or an aggregation pipeline.")
    print("Example Query: {'field': {'$gt': 10}}")
    print("Example Aggregation: [{'$match': {'field': {'$gt': 10}}}, {'$group': {'_id': '$category', 'count': {'$sum': 1}}}]")
    print("Type 'exit' to return to the main menu.")

    while True:
        raw_input_query = input("\nEnter your query or aggregation pipeline: ").strip()
        if raw_input_query.lower() == "exit":
            break

        try:
            query = eval(raw_input_query)

            if isinstance(query, dict):  # regular
                cursor = collection.find(query)
                results = list(cursor)
            elif isinstance(query, list):  # agg
                cursor = collection.aggregate(query)
                results = list(cursor)
            else:
                raise ValueError("Invalid input. Enter a valid JSON query or aggregation pipeline.")

            if results:
                print(f"\nResults ({len(results)} documents found):")
                for idx, doc in enumerate(results[:5], start=1):  # Limit to 5 results
                    print(f"{idx}: {json_util.dumps(doc, indent=2)}")
            else:
                print("\nNo documents found matching the query or pipeline.")
        except Exception as e:
            print(f"Error executing query or aggregation: {e}")

def main():
    print("Welcome to **ChatDB** - Your MongoDB query assistant!")
    print("------------------------------------------------------")
    print("Type 'exit' at any time to exit the application.")
    print("------------------------------------------------------")

    while True:
        # Prompt for database selection or upload
        print("\nOptions:")
        print("1. Select an existing database")
        print("2. Upload a new collection")
        print("3. Execute MongoDB Query Directly")
        print("Type 'exit' to quit.")

        user_choice = input("\nEnter your choice (1/2/3): ").strip()
        if user_choice.lower() == "exit":
            break
        if user_choice == "3":
            execute_direct_query(client)
        elif user_choice == "2":
            # Upload new collection
            db_name = input("\nEnter the name of the database to upload to (or type 'exit' to quit): ").strip()
            if db_name.lower() == "exit":
                continue

            collection_name = input("\nEnter the name of the collection to create (or type 'exit' to quit): ").strip()
            if collection_name.lower() == "exit":
                continue

            # Ask user for the file (detects type only json/csv allowed)
            file_type = input("\nEnter the file type ('csv' or 'json', or type 'exit' to quit): ").strip().lower()
            if file_type == "exit":
                continue

            if file_type not in ["csv", "json"]:
                print("Invalid file type. Please specify 'csv' or 'json'.")
                continue

            file_path = input(f"\nEnter the path to the {file_type.upper()} file to upload (or type 'exit' to quit): ").strip()
            if file_path.lower() == "exit":
                continue

            if file_type == "csv":
                data = convert_csv_to_json(file_path)
            elif file_type == "json":
                data = convert_json_to_dict(file_path)

            if data:
                upload_data_to_db(db_name, collection_name, data)
            continue  # Return to the main menu after upload

        elif user_choice == "1":
            # Select existing database
            databases = show_databases()
            user_db_choice = input("\nEnter a database name from the list above: ").strip()
            if user_db_choice.lower() == "exit":
                break
            if user_db_choice not in databases:
                print(f"Database '{user_db_choice}' not found.")
                continue

            # Select collections
            collections = show_collections(user_db_choice)
            user_col_choice = input("\nChoose a collection to work with: ").strip()
            if user_col_choice.lower() == "exit":
                break
            if user_col_choice not in collections:
                print(f"Collection '{user_col_choice}' not found!")
                continue

            db = client[user_db_choice]
            collection = db[user_col_choice]
            available_fields, field_types = get_available_fields(collection)

            print(f"\nPreview of the first item in '{user_col_choice}':")
            try:
                cursor = collection.find().limit(1)
                for idx, doc in enumerate(cursor, start=1):
                    print(f"{idx}: {json_util.dumps(doc, indent=2)}")
            except Exception as e:
                print(f"Error fetching preview: {e}")

            # Query loop
            while True:
                print(f"\nAvailable fields in '{user_col_choice}': {available_fields}")
                print(f"Field types: {field_types}")
                user_query = input("\nEnter your query (or type 'exit' to quit or type 'random' for random queries!): ").strip()
                user_query = user_query.replace(",", "").strip()
                if user_query.lower() == "exit":
                    break
                elif user_query.lower() == "random":
                    while True:
                        # Generate and display 5 random queries
                        random_queries = [generate_random_query(available_fields, field_types) for _ in range(5)]
                        print("\nGenerated Random Queries:")
                        for idx, query in enumerate(random_queries, start=1):
                            print(f"{idx}. {query}")

                        # Prompt user for selection
                        user_choice = input("\nSelect a query (1-5), type 'new' to regenerate queries, or 'exit' to return to the menu: ").strip().lower()

                        if user_choice == "exit":
                            break  # Exit random query generation loop
                        elif user_choice == "new":
                            continue  # Regenerate 5 new queries
                        elif user_choice.isdigit() and 1 <= int(user_choice) <= 5:
                            selected_query = random_queries[int(user_choice) - 1]
                            print(f"\nYou selected: {selected_query}")

                            try:
                                # Parse and execute the selected query
                                query, projection, sort_order, limit, skip, aggregation_pipeline = parse_query(
                                    selected_query, available_fields, field_types
                                )

                                if aggregation_pipeline:
                                    execute_query(collection, query=None, aggregation_pipeline=aggregation_pipeline)
                                else:
                                    execute_query(collection, query, projection, sort_order, limit, skip)

                            except ValueError as ve:
                                print(f"Error in query parsing: {ve}")
                            except Exception as e:
                                print(f"An unexpected error occurred: {e}")

                            break  # Exit after executing the selected query
                        else:
                            print("Invalid choice. Please try again.")

                else:
                    try:
                        query, projection, sort_order, limit, skip, aggregation_pipeline = parse_query(
                            user_query, available_fields, field_types
                        )

                        if aggregation_pipeline:
                            execute_query(collection, query=None, aggregation_pipeline=aggregation_pipeline)
                        else:
                            execute_query(collection, query, projection, sort_order, limit, skip)

                    except ValueError as ve:
                        print(f"Error in query parsing: {ve}")
                    except Exception as e:
                        print(f"An unexpected error occurred: {e}")

        else:
            print("Invalid choice. Please select 1, 2, or 3.")

# if __name__ == "__main__":
#     main()
