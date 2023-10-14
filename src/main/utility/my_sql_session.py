import mysql.connector

def mysql_cursor_create():
    cnx = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root"
    )
    cursor = cnx.cursor()  # Create a cursor inside the connection
    return cnx, cursor  # Return both the connection and the cursor

#connection, cursor = mysql_cursor_create()  # Get both the connection and cursor
