import sys
import os
import csv
import mysql.connector
from mysql.connector import Error

# Database configuration
db_config = {
    'host': 'localhost',
    'user': 'test',
    'password': 'password',
    'database': 'cs122a'
}

def get_connection():
    """Create and return a database connection."""
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

# Q1
def import_data(folder_name):
    """Delete existing tables, create new tables, import data from CSV files."""
    connection = get_connection()
    if not connection:
        print("Fail")
        return

    cursor = connection.cursor()

    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

        # Drop existing tables
        cursor.execute("SHOW TABLES;")
        for table_tuple in cursor.fetchall():
            cursor.execute(f"DROP TABLE IF EXISTS `{table_tuple[0]}`;")

        # Create tables (create_all_tables.sql)
        with open('create_all_tables.sql', 'r') as f:
            ddl_script = f.read()
            for statement in ddl_script.split(';'):
                if statement.strip():
                    cursor.execute(statement)

        # Import data from CSV files
        cursor.execute("SHOW TABLES;")
        new_tables = [table_tuple[0] for table_tuple in cursor.fetchall()]

        for table in new_tables:
            filename = f"{table}.csv"
            filepath = os.path.join(folder_name, filename)

            if os.path.exists(filepath):
                with open(filepath, 'r') as csvfile:
                    reader = csv.reader(csvfile)
                    data = list(reader)

                    if len(data) < 2:
                        continue  # No data to insert

                    num_cols = len(data[0])
                    placeholders = ', '.join(['%s'] * num_cols)
                    sql = f"INSERT IGNORE INTO `{table}` VALUES ({placeholders})"

                    for row in data[1:]:  # Skip header
                        cursor.execute(sql, row)

        # Commit changes and re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        connection.commit()
        print("Success")

    except Error:
        # Catch SQL errors
        print("Fail")
    except Exception:
        # Catch file errors or python errors
        print("Fail")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Q2
def insert_agent_client(uid, username, email, cardno, cardholder, 
                     expire, cvv, zip, interests):
    """Insert a new agent client into the related tables."""
    connection = get_connection()
    if not connection:
        print("Fail")
        return

    cursor = connection.cursor()

    try:
        # Check: if User doesn't exist, return. Else, continue to insert
        cursor.execute("SELECT uid FROM User WHERE uid = %s", (uid,))
        if cursor.fetchone() is None:
            print("Fail")
            return

        # Insert into AgentClient table
        agent_client_sql = """INSERT INTO AgentClient (uid, cardno, cardholder, 
                         expire, cvv, zip, interests)
                         VALUES (%s, %s, %s, %s, %s, %s, %s);"""
        cursor.execute(agent_client_sql, (uid, cardno, cardholder, 
                                         expire, cvv, zip, interests))

        connection.commit()
        print("Success")

    except Error:
        print("Fail")
    except Exception:
        print("Fail")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#Q3
def add_customized_model(mid, bmid):
    """Add a new customized model to the CustomizedModel table."""
    connection = get_connection()
    if not connection:
        print("Fail to connect to database")
        return

    cursor = connection.cursor()

    try:
        # Check: if BaseModel doesn't exist, return. Else, continue to add
        cursor.execute("SELECT bmid FROM BaseModel WHERE bmid = %s", (bmid,))
        if cursor.fetchone() is None:
            print("Fail")
            return

        # Insert into CustomizedModel table
        customized_model_sql = """INSERT INTO CustomizedModel (mid, bmid)
                                  VALUES (%s, %s);"""
        cursor.execute(customized_model_sql, (mid, bmid))

        connection.commit()
        print("Success")

    except Error:
        print("Fail")
    except Exception:
        print("Fail")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#Q4
def delete_base_model(bmid):
    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        # base model exists? fetches first row to check
        cursor.execute("SELECT 1 FROM BaseModel WHERE bmid = %s;", (bmid,))
        result = cursor.fetchone()

        if result is None:
            print("Base Model not detected.")
            return

        # try function now
        cursor.execute("DELETE FROM BaseModel WHERE bmid = %s;", (bmid,))
        connection.commit()

        if cursor.rowcount == 1:
            print("Success")
        else:
            print("Fail")

    except mysql.connector.Error:
        if connection:
            connection.rollback()
        print("Connection Failed.")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#Q5
def list_internet_service(bmid):
    """List all internet services that the model is utilizing, sorted by name, ascending."""
    connection = get_connection()
    if not connection:
        print("Fail to connect to database")
        return
    try:
        connection = get_connection()
        cursor = connection.cursor()

        # Check: if BaseModel doesn't exist, return. Else, continue to list
        cursor.execute("SELECT bmid FROM BaseModel WHERE bmid = %s", (bmid,))
        if cursor.fetchone() is None:
            print("Fail, base model not found")
            return

        # From ModelServices, find all row with bmid and select sid
        cursor.execute("""
        SELECT sid, endpoints, provider
        FROM InternetService
        WHERE sid IN (
            SELECT sid
            FROM ModelServices
            WHERE bmid = %s
        )
        ORDER BY provider ASC;
        """, (bmid,))

        # Print result table: sid, endpoints, provider
        rows = cursor.fetchall()
        if rows:
            for row in rows:
                print(f"{row[0]},{row[1]},{row[2]}")

    except Error as e:
        print(f"Fail, {e}")
    except Exception as e:
        print(f"Fail, {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#Q6

#Q7

#Q8


#--TEST FUNCTIONS--#
def test_connection():
    """Test database connection."""
    connection = get_connection()
    if connection:
        print("Success")
        connection.close()
    else:
        print("Fail")

def test_insert_table():
    connection = get_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY, name VARCHAR(50));")
        cursor.execute("INSERT INTO test_table (id, name) VALUES (1, 'Test Name');")
        connection.commit()
        print("Success")
    except Error:
        print("Fail")
    finally:
        # cursor.execute("DROP TABLE IF EXISTS test_table;")
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def delete_all_tables():
    connection = get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute("SHOW TABLES;")
        for table_tuple in cursor.fetchall():
            cursor.execute(f"DROP TABLE IF EXISTS `{table_tuple[0]}`;")
        
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        print("Success")
    except Error:
        print("Fail to insert")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
#--END OF TEST FUNCTIONS--#


def main():
    if len(sys.argv) < 2:
        return

    function_name = sys.argv[1]
    
    # Handle NULL inputs for arguments
    raw_args = sys.argv[2:]
    args = [None if x == "NULL" else x for x in raw_args]

    if function_name == "import":
        if len(args) == 1:
            import_data(args[0])
        else:
            print("Fail")
    
    elif function_name == "insertAgentClient":
        if len(args) == 9:
            insert_agent_client(
                args[0], # uid
                args[1], # username (Ignored)
                args[2], # email (Ignored)
                args[3], # cardno
                args[4], # cardholder
                args[5], # expire
                args[6], # cvv
                args[7], # zip
                args[8]  # interests
            )
        else:
            print("Fail, incorrect number of arguments")

    elif function_name == "addCustomizedModel":
        if len(args) == 2:
            add_customized_model(args[0], args[1])
        else:
            print("Fail")

    elif function_name == "deleteBaseModel":
        if len(args) == 1:
            delete_base_model(args[0])
        else:
            print("Fail")
    
    elif function_name == "listInternetService":
        if len(args) == 1:
            list_internet_service(args[0])
        else:
            print("Fail")

    elif function_name == "test_connection":
        test_connection()
    elif function_name == "test_insert_table":
        test_insert_table()
    elif function_name == "delete_all_tables":
        delete_all_tables()

    else:
        print("Fail, function not found")

if __name__ == "__main__":
    main()