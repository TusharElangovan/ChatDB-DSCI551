
from FinalSQLFlow import sql_workflow_manager
from FinalNoSQLFlow import main
import time
from datetime import datetime

# Function for SQL operations (placeholder)
def handle_sql_operations():
    print("-"*23, "End of SQL Tasks", "-"*23)
    print("\nWelcome to the SQL section!")
    sql_workflow_manager()
    # Add SQL-related operations here
    print("-"*23, "End of SQL Tasks", "-"*23)
    

# Function for NoSQL operations (placeholder)
def handle_nosql_operations():
    print("-"*23, "START of NoSQL Tasks", "-"*23)
    print("\nWelcome to the NoSQL section!")
    main()
    # Add NoSQL-related operations here
    print("-"*23, "End of NoSQL Tasks", "-"*23)
    

# Function to greet the user and display time/date
def greet_user():
    # Get the current date and time
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Print the greeting message
    print("\n" + "="*50)
    print("Good day! The current date and time is:", current_time)
    print("="*50)
    
    # Welcome to ChaTDB
    print("\nWelcome to ChaTDB! Your one-stop solution for database operations.\n")
    

# Function to show main menu and get the user's choice
def main_menu():
    print("Would you like to work with an SQL or NoSQL database?")
    print("1. SQL")
    print("2. NoSQL")
    print("3. Exit")

    while True:
        choice = input("\nPlease enter 1 for SQL, 2 for NoSQL, or 3 to Exit: ").strip()
        
        if choice == "1":
            handle_sql_operations()  # Call SQL-related function
            break
        elif choice == "2":
            handle_nosql_operations()  # Call NoSQL-related function
            break
        elif choice == "3":
            print("\nExiting the program. Goodbye!")
            exit()  # Exit the program
        else:
            print("\nInvalid choice. Please enter 1 for SQL, 2 for NoSQL, or 3 to Exit.")

# Function to ask the user if they want to go back to the main menu or exit
def ask_for_back_option():
    while True:
        choice = input("\nDo you want to go back to the main menu? (y/n): ").strip().lower()
        if choice == 'y':
            return True  # Return to the main menu
        elif choice == 'n':
            print("\nExiting the program. Goodbye!")
            print("-"*23, "Thank You", "-"*23)
            exit()  # Exit the program
        else:
            print("\nInvalid input. Please enter 'y' for yes or 'n' for no.")

def mainDriver():
    # Greet the user
    greet_user()

    while True:
        # Show the main menu
        main_menu()

        # Ask if the user wants to go back to the main menu after completing operations
        if ask_for_back_option():
            print("\nReturning to the main menu...\n")
            continue  # Restart the loop and show the main menu
        else:
            break  # Exit the loop and end the program

# Entry point of the script
if __name__ == "__main__":
    mainDriver()
