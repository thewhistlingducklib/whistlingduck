from typing import List, Dict, Union, Optional, Any
import duckdb
import polars as pl
import uuid
from datetime import datetime, timezone
from duckdb import DuckDBPyConnection

def CountDistinct(dataset: Any, 
                 column_list: Optional[List[str]] = None,
                 filter_condition_dict: Optional[Dict[str, Union[str, int, float]]] = None,
                 dataset_filter_by_data_type: Optional[List[str]] = None,
                 duckdb_connection: Optional[DuckDBPyConnection] = None
                ) -> List[Dict[str, Union[str, int, dict, None]]]:
    """
   Calculate distinct count metrics for specified columns in a dataset using DuckDB.
    
    This function counts the number of unique values in specified columns. It can filter columns 
    by explicitly provided column names AND/OR by data type(s). The function processes the data 
    using DuckDB for efficient computation of distinct counts.
    
    Example:
        Consider a column 'products' with values: ["apple", "banana", "apple", "orange", "banana"]
        - Distinct count would be 3 (unique values: "apple", "banana", "orange")
        
        >>> df = pd.DataFrame({'products': ["apple", "banana", "apple", "orange", "banana"]})
        >>> CountDistinct(df, column_list=['products'])
        [
            {
                'column_name': 'products',
                'distinct_count': 3,
                'table_name': 'countdistinct_xyz123',
                'execution_timestamp_utc': '2024-01-26 12:34:56',
                'filter_conditions': None,
                'filtered_by_data_type': None
            }
        ]
    
    Returns:
        List[Dict[str, Union[str, int, dict, None]]]: A list of dictionaries with the following keys:
            - column_name (str): Name of the analyzed column
            - distinct_count (int): Count of unique/distinct values in the column
            - table_name (str): Name of the analyzed table (original or temporary)
            - execution_timestamp_utc (str): Timestamp of execution in UTC format
            - filter_conditions (dict|None): Applied filter conditions if any
            - filtered_by_data_type (list|None): Data types used for filtering if any

    Args:
        dataset (Any): Input dataset that can be either:
            - A DataFrame (pandas, polars) or other DuckDB-compatible data structure
            - A string representing an existing table name in the DuckDB connection
            When providing a DataFrame along with duckdb_connection, the DataFrame will be
            registered as a temporary table in that connection.
            
        column_list (Optional[List[str]], optional): List of column names to analyze.
            Can be used together with dataset_filter_by_data_type. Defaults to None.
            Example: ['column1', 'column2']
            
        filter_condition_dict (Optional[Dict[str, Union[str, int, float]]], optional):
            Dictionary of filter conditions to apply before calculating distinct counts.
            Format: {'column_name': value}. Supports string, integer, and float values.
            Example: {'category': 'electronics', 'price': 100}
            Defaults to None.
            
        dataset_filter_by_data_type (Optional[List[str]], optional): 
            Data type(s) to filter columns. Can be used together with column_list.
            The function will analyze all columns of these data types.
            Example: ['VARCHAR'] or ['VARCHAR', 'INTEGER']
            Defaults to None.
            
        duckdb_connection (Optional[DuckDBPyConnection], optional): Existing DuckDB connection.
            If None, a new connection will be created and closed after execution.
            Can be used with either a table name string or a DataFrame input.
            Defaults to None.
            
    Raises:
        ValueError: If neither column_list nor dataset_filter_by_data_type is provided
        ValueError: If the dataset cannot be registered with DuckDB
        ValueError: If specified columns are not found in the dataset
        ValueError: If filter conditions reference non-existent columns
    """
    # Generate UUID for table name and get UTC timestamp
    unique_id = str(uuid.uuid4()).replace('-', '_')
    timestamp = datetime.now(timezone.utc)
    temp_table_name = f"countdistinct_{unique_id}"
    
    if column_list is None and dataset_filter_by_data_type is None:
        raise ValueError(
            "Please provide either a list of columns using column_list or specify "
            "data type(s) using dataset_filter_by_data_type."
        )
    
    # Handle DuckDB connection and table registration
    if duckdb_connection is None:
        con = duckdb.connect()
        try:
            con.register(temp_table_name, dataset)
            source_table = temp_table_name
        except Exception as e:
            con.close()
            raise ValueError(f"Failed to register dataset: {str(e)}")
    else:
        con = duckdb_connection
        if isinstance(dataset, str):
            try:
                con.sql(f"PRAGMA table_info('{dataset}')")
                source_table = dataset
            except duckdb.CatalogException:
                raise ValueError(f"Table '{dataset}' does not exist in the DuckDB connection")
        else:
            try:
                con.register(temp_table_name, dataset)
                source_table = temp_table_name
            except Exception as e:
                raise ValueError(f"Failed to register dataset with existing connection: {str(e)}")
    
    # Get table info
    dtype_info = con.sql(f"PRAGMA table_info('{source_table}')").pl()
    dataset_columns = dtype_info['name'].to_list()

    # Initialize final column list
    final_column_list = set()
    
    # Validate column list if provided
    if column_list:
        if not isinstance(column_list, list):
            raise ValueError("column_list must be a list of strings")
        invalid_cols = set(column_list) - set(dataset_columns)
        if invalid_cols:
            raise ValueError(f"These columns were not found in the dataset: {', '.join(invalid_cols)}")
        final_column_list.update(column_list)
    
    # Handle data type filtering
    if dataset_filter_by_data_type:
        if not isinstance(dataset_filter_by_data_type, list):
            raise ValueError("dataset_filter_by_data_type must be a list of strings")
        
        data_type_columns = dtype_info.filter(
            pl.col("type").str.to_uppercase().is_in([dt.upper() for dt in dataset_filter_by_data_type])
        )['name'].to_list()
        
        if not data_type_columns:
            raise ValueError(f"No columns found of types {dataset_filter_by_data_type}")
        
        final_column_list.update(data_type_columns)
    
    # Convert set back to list
    final_column_list = list(final_column_list)
    
    # Handle filter conditions
    if filter_condition_dict:
        if not isinstance(filter_condition_dict, dict):
            raise ValueError("filter_condition_dict must be a dictionary")
        invalid_filter_cols = list(set(filter_condition_dict.keys()) - set(dataset_columns))
        if invalid_filter_cols:
            raise ValueError(f"These columns were not found in your dataset: {', '.join(invalid_filter_cols)}")
            
        where_clause = "WHERE " + " AND ".join(
            f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}"
            for col, val in filter_condition_dict.items()
        )
    else:
        where_clause = ""
    
    # Generate and execute SQL queries
    sql_statements = [
        f"""
        SELECT 
            '{column}' as column_name,
            COUNT(DISTINCT {column}) as distinct_count
        FROM {source_table}
        {where_clause}
        """
        for column in final_column_list
    ]
    
    sql_query = " UNION ALL ".join(sql_statements)
    result = con.sql(sql_query).pl()
    
    if duckdb_connection is None:
        con.close()
    
    results = result.select([
        pl.col('column_name'),
        pl.col('distinct_count').cast(pl.Int64)
    ]).to_dicts()
    
    for result in results:
        result.update({
            'table_name': source_table,
            'execution_timestamp_utc': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            'filter_conditions': filter_condition_dict if filter_condition_dict else None,
            'filtered_by_data_type': dataset_filter_by_data_type if dataset_filter_by_data_type else None
        })
    
    return results