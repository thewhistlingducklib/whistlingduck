# WhistlingDuck Read Into Table Function Reference

## Function Index 
| Function | User Summary | Dev Summary
| --- | --- | --- |
| [read_csv_into_table](#read_csv_into_table) | Imports a CSV file into a DuckDB table and returns the connection, table name, and table schema information in a JSON format.The DuckDB connection returned in the output can be used for further queries.| The function uses DuckDB's `read_csv_auto` function to automatically infer the CSV schema.


### read_csv_into_table

```
Args:
    file_path (str): The file path of the CSV file to read.
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
```
#### Example

```python
table_info = wd.read_csv_into_table('survey_results_public.csv', "survey_results_public")
table_info['duckdb_connection'].sql(f" from {table_info['table_name']}").show()
```

