#pip install mysql-connector-python 
import mysql.connector

def get_cursor():
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="****",
        database="database_name"
    )
    return conn.cursor(), conn


'''
SQL TO CREATE THE TABLE:

CREATE TABLE orders (
  order_id VARCHAR(50),
  order_date DATE,
  product_id VARCHAR(50),
  quantity INT,
  sales INT,
  city VARCHAR(50)
);

'''