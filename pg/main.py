import psycopg2
from psycopg2.extras import execute_values


def get_pg_connection(**kwargs):
    """
    Connects to a PostgreSQL database using the provided keyword arguments and returns a database connection object.

    Args:
    - **kwargs: keyword arguments containing the connection parameters, such as host, port, database, user, and password

    Returns:
    A psycopg2.extensions.connection object representing the database connection
    """
    conn = psycopg2.connect(**kwargs)
    return conn


def create_table(conn, table_name, field_dict, close_conn=True) -> bool:
    """
    Creates a new table with the specified name and fields using the provided database connection.

    Args:
    - conn (psycopg2.extensions.connection): a database connection object
    - table_name (str): the name of the table to be created
    - field_dict (dict): a dictionary containing the names of the fields and their corresponding data types

    Returns:
    True if a table was created successfully, False otherwise
    """

    cursor = conn.cursor()

    # Generate the SQL command to create the table
    command = f"CREATE TABLE {table_name} ("
    command += "id SERIAL PRIMARY KEY, "

    for field, datatype in field_dict.items():
        command += f"{field} {datatype} NULL, "

    # Remove the last comma and space from the command
    command = command[:-2]

    # Close the parentheses and execute the command
    command += ")"

    try:
        cursor.execute(command)
    except Exception as e:
        print(f"Error creating table: {e}")
        return False

    # Commit the changes and close the connection
    conn.commit()
    if close_conn:
        conn.close()
    cursor.close()

    return True


def insert_df_into_postgresql(conn, table_name, dataframe) -> None:
    """
    Inserts all values in a Pandas DataFrame to a table in a PostgreSQL database.

    Args:
    - conn (psycopg2.extensions.connection): The connection to the database.
    - table_name (str): The name of the table to insert the values into.
    - dataframe (pandas.DataFrame): The DataFrame to be inserted.

    Returns:
    None

    Raises:
    Exception: If there is an error during insertion.
    """
    try:
        # Prepare the query
        query = f"INSERT INTO {table_name} ({', '.join(dataframe.columns)}) VALUES %s"
        values = [tuple(x) for x in dataframe.to_numpy()]

        # Execute the query
        cur = conn.cursor()
        execute_values(cur, query, values)
        conn.commit()

        # Close the cursor
        cur.close()
    except Exception as e:
        raise Exception(f"Error inserting data into PostgreSQL: {str(e)}")
