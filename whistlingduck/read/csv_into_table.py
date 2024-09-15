import duckdb # type: ignore
from pydantic import FilePath, validate_call # type: ignore

@validate_call
def read_csv_into_table(file_path: FilePath, table_name: str) -> dict:
    """
    Imports a CSV file into a DuckDB table and returns the connection, table name, and table schema information.

    Args:
        file_path (FilePath): The file path of the CSV file to read. Must be a valid file path.
        table_name (str): The name of the table to create in DuckDB.

    Returns:
        dict: A dictionary containing the following keys:
            - 'duckdb_connection' (duckdb.DuckDBPyConnection): The DuckDB connection object.
            - 'table_name' (str): The name of the created table.
            - 'table_schema' (list of dict): A list where each dictionary represents a column in the table with the following keys:
                - 'cid' (int): The column ID (starting from 0).
                - 'name' (str): The name of the column.
                - 'type' (str): The data type of the column.
                - 'notnull' (int): Indicates if the column is NOT NULL (1) or NULLABLE (0).
                - 'dflt_value' (Any): The default value of the column if specified, otherwise None.
                - 'pk' (int): Indicates if the column is part of the primary key (1) or not (0).

    Raises:
        pydantic.error_wrappers.ValidationError: If the input arguments do not match the specified types.
        duckdb.Error: If there is an error executing DuckDB commands.

    Notes:
        - The function uses DuckDB's `read_csv_auto` function to automatically infer the CSV schema.
        - The DuckDB connection returned in the output can be used for further queries.
    """
    con = duckdb.connect()
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')")
    con.execute(f"PRAGMA table_info({table_name})")
    schema_info = con.pl()  # Polars DataFrame
    schema_info_dicts = schema_info.to_dicts()

    return {
        'duckdb_connection': con,
        'table_name': table_name,
        'table_schema': schema_info_dicts
    }
