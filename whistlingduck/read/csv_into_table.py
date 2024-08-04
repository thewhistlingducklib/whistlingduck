from pydantic import BaseModel, FilePath, ConfigDict
import duckdb

class CSVToDuckDBInput(BaseModel):
    file_path: FilePath
    table_name: str

class CSVToDuckDBTableDetailsOutput(BaseModel):
    duckdb_connection: duckdb.DuckDBPyConnection
    table_name: str
    table_schema: list
    model_config = ConfigDict(arbitrary_types_allowed=True)

def read_csv_into_table(file_path: str, table_name: str) -> dict:
    """
    Imports a CSV file into a DuckDB table and returns the connection, table name, and table schema information.
    
    Args:
        file_path (str): The file path of the CSV file to read.
        table_name (str): The name of the table to create in DuckDB.
        
    Returns:
        dict: A dictionary containing the DuckDB connection, table name, and table schema information.
            The dictionary has the following keys:
                - 'duckdb_connection' (duckdb.DuckDBPyConnection): The DuckDB connection object.
                - 'table_name' (str): The name of the created table.
                - 'table_schema' (list): A list of dictionaries, each representing a column in the table with 
                  the following keys:
                    - 'cid' (int): The column ID.
                    - 'name' (str): The column name.
                    - 'type' (str): The column data type.
                    - 'notnull' (bool): Whether the column is NOT NULL.
                    - 'pk' (bool): Whether the column is a primary key.
                    
    Raises:
        ValueError: If the file path is not valid.
        duckdb.Error: If there is an error executing the DuckDB commands.
    """
    input_data = CSVToDuckDBInput(file_path=file_path, table_name=table_name)
    con = duckdb.connect()
    
    # Create table from CSV
    create_table_query = f"CREATE TABLE {input_data.table_name} AS SELECT * FROM read_csv_auto('{input_data.file_path}')"
    con.execute(create_table_query)
    
    # Retrieve table schema information
    schema_query = f"PRAGMA table_info({input_data.table_name})"
    schema_info = con.execute(schema_query).fetchall()
    
    # Convert schema information to list of dictionaries
    schema_info_dicts = [
        {
            "cid": column[0],
            "name": column[1],
            "type": column[2],
            "notnull": bool(column[3]),
            "pk": bool(column[5])
        }
        for column in schema_info
    ]
    
    output = CSVToDuckDBTableDetailsOutput(
        duckdb_connection=con, 
        table_name=input_data.table_name, 
        table_schema=schema_info_dicts
    )
    return output.dict()
