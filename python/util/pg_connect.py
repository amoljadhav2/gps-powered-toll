import psycopg2
from psycopg2 import sql
import psycopg2.pool

# Database connection configuration
DB_HOST = 'postgres.cbqwc6simkya.us-east-1.rds.amazonaws.com'
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'semicolon24'
DB_PORT ='5432'

def connect_to_db():
   """Establishes a connection to the PostgreSQL database."""
   try:
       conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, port=DB_PORT, user=DB_USER, password=DB_PASSWORD)
       return conn
   except psycopg2.Error as e:
       print("Error connecting to the database:", e)
       return None

def connect_db_pool(username, password, database_host, database_port, database_name):
    postgre_sql_pool = psycopg2.pool.SimpleConnectionPool(1, 20, database=database_name, user=username,
                                                         password=password, host=database_host, port=database_port)
    ps_connection = postgre_sql_pool.getconn()
    ps_cursor = ps_connection.cursor()
    return postgre_sql_pool, ps_connection, ps_cursor

def execute_query(conn, query, params=None):
   """
   Executes a SQL query on the database.
   Args:
       conn: Database connection object.
       query: SQL query string.
       params: Optional parameters for the query.
   Returns:
       List of tuples containing the query results.
   """
   try:
       cur = conn.cursor()
       if params:
           cur.execute(query, params)
       else:
           cur.execute(query)
       result = cur.fetchall()
       conn.commit()
       cur.close()
       return result
   except psycopg2.Error as e:
       print("Error executing query:", e)
       return None
def execute_insert(conn, table, data):
   """
   Inserts data into a specified table.
   Args:
       conn: Database connection object.
       table: Name of the table.
       data: Dictionary containing column names and corresponding values.
   Returns:
       True if the insert operation was successful, False otherwise.
   """
   try:
       columns = data.keys()
       values = [data[col] for col in columns]
       query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
           sql.Identifier(table),
           sql.SQL(', ').join(map(sql.Identifier, columns)),
           sql.SQL(', ').join([sql.Literal(value) for value in values])
       )
       cur = conn.cursor()
       cur.execute(query)
       conn.commit()
       cur.close()
       return True
   except psycopg2.Error as e:
       print("Error inserting data:", e)
       return False
