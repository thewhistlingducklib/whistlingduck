def read_csv_into_table_doc ():
    doc = """ 
    This function reads a CSV file and creates a DuckDB table. 
    It takes two arguments: the file path and the table name. 
    The function returns a dictionary containing the DuckDB connection, the table name, and the table schema information (as a list of dictionaries). 
    Keys to access the outputs -
     - 'duckdb_connection' (duckdb.DuckDBPyConnection): The DuckDB connection object.
     - 'table_name' (str): The name of the created table.
     - 'table_schema' (list): A list of dictionaries, each representing a column in the table with 
            the following keys:
            - 'cid' (int): The column ID.
            - 'name' (str): The column name.
            - 'type' (str): The column data type.
            - 'notnull' (bool): Whether the column is NOT NULL.
            - 'pk' (bool): Whether the column is a primary key.

    """
    return doc
