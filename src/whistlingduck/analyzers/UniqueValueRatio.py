from typing import List, Dict, Union, Optional, Any
import duckdb
import polars as pl
import uuid
from datetime import datetime, timezone
from duckdb import DuckDBPyConnection

def UniqueValueRatio(dataset: Any, 
                    column_list: Optional[List[str]] = None, 
                    filter_condition_dict: Optional[Dict[str, Union[str, int, float]]] = None,
                    dataset_filter_by_data_type: Optional[List[str]] = None,
                    duckdb_connection: Optional[DuckDBPyConnection] = None,
                    decimal_places: int = 2
                   ) -> List[Dict[str, Union[str, int, float, dict, None]]]:
    """
   Calculate unique value ratio metrics for specified columns in a dataset using DuckDB.
    
    This function calculates the ratio of values that appear exactly once to the total
    number of distinct values in specified columns.
    
    Example:
        Consider a column 'colors' with values: ["red", "blue", "red", "green", "blue", "yellow"]
        
        UniqueValueRatio calculation:
        - Values appearing exactly once: "green", "yellow" (count = 2)
        - Distinct values: "red", "blue", "green", "yellow" (count = 4)
        - UniqueValueRatio = 2/4 = 0.5 (50%)

    Returns:
        List[Dict[str, Union[str, int, float, dict, None]]]: A list of dictionaries with the following keys:
            - column_name (str): Name of the analyzed column
            - unique_count (int): Count of values that appear exactly once
            - distinct_count (int): Count of distinct values
            - unique_value_ratio (float): Ratio of unique_count to distinct_count
            - table_name (str): Name of the analyzed table
            - execution_timestamp_utc (str): Timestamp of execution in UTC
            - filter_conditions (dict|None): Applied filter conditions if any
            - filtered_by_data_type (list|None): Data types used for filtering if any

    Args:
        dataset (Any): Input dataset that can be either:
            - A DataFrame (pandas, polars) or other DuckDB-compatible data structure
            - A string representing an existing table name in the DuckDB connection
        column_list (Optional[List[str]], optional): List of column names to analyze.
        filter_condition_dict (Optional[Dict[str, Union[str, int, float]]], optional):
            Dictionary of filter conditions to apply before calculating unique value ratio.
        dataset_filter_by_data_type (Optional[List[str]], optional): 
            Data type(s) to filter columns.
        duckdb_connection (Optional[DuckDBPyConnection], optional): Existing DuckDB connection.
        decimal_places (int, optional): Number of decimal places to round the ratio.
    
    """
    if decimal_places < 0:
        raise ValueError("decimal_places must be non-negative")
        
    # Generate UUID for table name and get UTC timestamp
    unique_id = str(uuid.uuid4()).replace('-', '_')
    timestamp = datetime.now(timezone.utc)
    temp_table_name = f"unique_value_ratio_{unique_id}"
    
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
            raise ValueError(f"Failed to register dataset: {str(e)}. Please ensure the dataset is in a DuckDB-compatible format.")
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
                raise ValueError(
                    f"Failed to register dataset with existing connection: {str(e)}. "
                    "Please ensure the dataset is in a DuckDB-compatible format."
                )
    
    # Get table info for column validation
    dtype_info = con.sql(f"PRAGMA table_info('{source_table}')").pl()
    dataset_columns = dtype_info['name'].to_list()

    # Initialize final column list
    final_column_list = set()
    
    # Validate column list if provided
    if column_list:
        if not isinstance(column_list, list):
            raise ValueError(
                "column_list must be a list of strings. "
                "For single column, use ['column_name'] instead of 'column_name'."
            )
        invalid_cols = set(column_list) - set(dataset_columns)
        if invalid_cols:
            raise ValueError(
                f"These columns were not found in the dataset: {', '.join(invalid_cols)}. "
                "Please verify the column names."
            )
        final_column_list.update(column_list)
    
    # Handle data type filtering
    if dataset_filter_by_data_type:
        if not isinstance(dataset_filter_by_data_type, list):
            raise ValueError(
                "dataset_filter_by_data_type must be a list of strings. "
                "For single data type, use ['VARCHAR'] instead of 'VARCHAR'."
            )
        data_types = dataset_filter_by_data_type
        
        data_type_columns = dtype_info.filter(
            pl.col("type").str.to_uppercase().is_in([dt.upper() for dt in data_types])
        )['name'].to_list()
        
        if not data_type_columns:
            raise ValueError(
                f"We couldn't find any columns of types {data_types}. "
                "You might want to check the data types or consider specifying columns directly using column_list."
            )
        
        final_column_list.update(data_type_columns)
    
    # Convert set back to list
    final_column_list = list(final_column_list)
    
    # Handle filter conditions
    if filter_condition_dict:
        if not isinstance(filter_condition_dict, dict):
            raise ValueError(
                "filter_condition_dict must be a dictionary. "
                "For single filter condition, use {'column_name': value} instead of a single value."
            )
        invalid_filter_cols = list(set(filter_condition_dict.keys()) - set(dataset_columns))
        if invalid_filter_cols:
            raise ValueError(
                f"We couldn't find these columns in your dataset: {', '.join(invalid_filter_cols)}. "
                "Please verify the column names in your filter conditions."
            )
            
        where_clause = "WHERE " + " AND ".join(
            f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}"
            for col, val in filter_condition_dict.items()
        )
    else:
        where_clause = ""
    
    # Generate SQL queries for unique value ratio calculation
    sql_statements = [
        f"""
        SELECT 
            '{column}' as column_name,
            SUM(CASE WHEN value_count = 1 THEN 1 ELSE 0 END) as unique_count,
            COUNT(*) as distinct_count,
            ROUND(
                CAST(SUM(CASE WHEN value_count = 1 THEN 1 ELSE 0 END) AS FLOAT) / 
                NULLIF(COUNT(*), 0),
                {decimal_places}
            ) as unique_value_ratio
        FROM (
            SELECT {column}, COUNT(*) as value_count
            FROM {source_table}
            {where_clause}
            GROUP BY {column}
        ) t
        """
        for column in final_column_list
    ]
    
    sql_query = " UNION ALL ".join(sql_statements)
    result = con.sql(sql_query).pl()
    
    if duckdb_connection is None:
        con.close()
    
    # Format results
    results = result.select([
        pl.col('column_name'),
        pl.col('unique_count').cast(pl.Int64),
        pl.col('distinct_count').cast(pl.Int64),
        pl.col('unique_value_ratio').cast(pl.Float64)
    ]).to_dicts()
    
    # Add metadata to results
    for result in results:
        result.update({
            'table_name': source_table,
            'execution_timestamp_utc': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            'filter_conditions': filter_condition_dict if filter_condition_dict else None,
            'filtered_by_data_type': dataset_filter_by_data_type if dataset_filter_by_data_type else None
        })
    
    return results