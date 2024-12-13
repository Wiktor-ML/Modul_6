import sqlite3
from sqlite3 import Error
import os

def create_connection(db_file):
    """ Create a database connection to the SQLite database specified by db_file. """
    conn = None
    db_exists = os.path.exists(db_file)  # Check if database file exists
    try:
        conn = sqlite3.connect(db_file)
        print(f"{'Connected to' if db_exists else 'Created new'} database: {db_file}")
        return conn
    except Error as e:
        print(e)
    return conn

def execute_sql(conn, sql):
    """ 
    Execute a SQL statement.
    :param conn: Connection object to the SQLite database
    :param sql: The SQL statement to be executed
    :return: None
    """
    try:
        # Create a cursor object to execute the SQL command
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        # Print error message if SQL execution fails
        print(e)

if __name__ == "__main__":

    # SQL statement to create the 'clients' table
    create_clients_sql = """
    CREATE TABLE IF NOT EXISTS clients (
       client_id INTEGER PRIMARY KEY,  
       first_name VARCHAR(50) NOT NULL,     
       last_name VARCHAR(50) NOT NULL,         
       email VARCHAR(100) NOT NULL          
    );
    """

    # SQL statement to create the 'orders' table
    create_orders_sql = """
    CREATE TABLE IF NOT EXISTS orders (
       order_id INTEGER PRIMARY KEY,    
       order_date DATE NOT NULL,     
       amount DECIMAL(10,2) NOT NULL,
       client_id INTEGER,
       FOREIGN KEY (client_id) REFERENCES clients (client_id)  
    );
    """

    
    try:
        conn = create_connection("database_clients_orders.db")
        if conn is not None:  
            # Create tables
            execute_sql(conn, create_clients_sql)
            execute_sql(conn, create_orders_sql)

            # Insert a client to ensure the foreign key constraint is satisfied
            cursor = conn.cursor()
            cursor.execute('INSERT INTO clients (first_name, last_name, email) VALUES (?, ?, ?)',("John", "Doe", "john.doe@example.com"))
            
            # Insert an order
            cursor.execute('INSERT INTO orders(order_date, amount, client_id) VALUES(?,?,?)', ("2020-05-08 00:00:00", 200, 1))  # Use the correct client_id

            # Update order
            cursor.execute('UPDATE orders SET order_date = ?, amount = ?, client_id = ? WHERE order_id = ?', ('2024-05-11 11:11:01',300,1,1))
            
            # Delete order
            cursor.execute('DELETE FROM orders WHERE order_id = ?', (2,))
            
            # Commit the commands            
            conn.commit()  
            
            # Query data
            cursor.execute('SELECT * FROM orders')
            rows = cursor.fetchall()
            for row in rows:
                print(row)         
            
    except Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()    