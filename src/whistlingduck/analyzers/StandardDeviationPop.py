from typing import List, Dict, Union, Optional, Any
import duckdb
import polars as pl
import uuid
from datetime import datetime, timezone
from duckdb import DuckDBPyConnection

def StandardDeviationPop(dataset: Any, 
                     column_list: Optional[List[str]] = None, 
                     filter_condition_dict: Optional[Dict[str, Union[str, int, float]]] = None,
                     dataset_filter_by_data_type: Optional[List[str]] = None,
                     duckdb_connection: Optional[DuckDBPyConnection] = None,
                     decimal_places: int = 4
                    ) -> List[Dict[str, Union[str, float, dict, None]]]:
    """
    Calculate population standard deviation for specified numeric columns in a dataset using DuckDB.
    
    This function computes the population standard deviation for specified numeric columns using DuckDB's
    STDDEV_POP function. It can analyze either explicitly specified columns or filter columns by data
    type(s), with optional filtering conditions on the dataset.
    
    Example:
        Consider a column 'salaries' with values: [50000, 60000, 55000, 65000, 52000]
        Standard deviation = sqrt(sum((x - mean)^2) / N)
        Result ≈ 5505.43
    
    Returns:
        List[Dict[str, Union[str, float, dict, None]]]: A list of dictionaries with the following keys:
            - column_name (str): Name of the analyzed column
            - stddev_value (float): Calculated population standard deviation
            - table_name (str): Name of the analyzed table
            - execution_timestamp_utc (str): Timestamp of execution in UTC
            - filter_conditions (dict|None): Applied filter conditions if any
            - filtered_by_data_type (list|None): Data types used for filtering if any
    
    Args:
        dataset (Any): Input dataset that can be either:
            - A DataFrame (pandas, polars) or other DuckDB-compatible data structure
            - A string representing an existing table name in the DuckDB connection
            When providing a DataFrame along with duckdb_connection, the DataFrame will be
            registered as a temporary table in that connection.
            
        column_list (Optional[List[str]], optional): List of numeric column names to analyze.
            Can be used together with dataset_filter_by_data_type. Defaults to None.
            
        filter_condition_dict (Optional[Dict[str, Union[str, int, float]]], optional):
            Dictionary of filter conditions to apply before calculating standard deviation.
            Format: {'column_name': value}. Supports string, integer, and float values.
            Example: {'department': 'IT', 'age': 30}. Defaults to None.
            
        dataset_filter_by_data_type (Optional[List[str]], optional): 
            Data type(s) to filter columns. Can be a single type as string or list of types.
            Can be used together with column_list. The function will analyze all numeric
            columns of these data types. Case-insensitive. Defaults to None.
            Example: ['INTEGER'] or ['INTEGER', 'DOUBLE']
            
        duckdb_connection (Optional[DuckDBPyConnection], optional): Existing DuckDB connection.
            If None, a new connection will be created and closed after execution.
            Can be used with either a table name string or a DataFrame input.
            Defaults to None.
            
        decimal_places (int, optional): Number of decimal places to round the standard
            deviation values. Defaults to 4.
    
    Raises:
        ValueError: If any of the following conditions are met:
            - Neither column_list nor dataset_filter_by_data_type is provided
            - decimal_places is negative
            - Invalid column names in column_list
            - No columns found matching the specified data types
            - Non-numeric columns specified for standard deviation calculation
            - Invalid filter conditions
            - Failed to register or access the dataset
    """
    
    if decimal_places < 0:
        raise ValueError("decimal_places must be non-negative")
        
    unique_id = str(uuid.uuid4()).replace('-', '_')
    timestamp = datetime.now(timezone.utc)
    temp_table_name = f"stddev_{unique_id}"
    
    if column_list is None and dataset_filter_by_data_type is None:
        raise ValueError(
            "Please provide either a list of columns using column_list or specify "
            "data type(s) using dataset_filter_by_data_type."
        )
    
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
    
    dtype_info = con.sql(f"PRAGMA table_info('{source_table}')").pl()
    dataset_columns = dtype_info['name'].to_list()

    final_column_list = set()
    
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
    
    numeric_types = {'INTEGER', 'BIGINT', 'DOUBLE', 'REAL', 'DECIMAL', 'NUMERIC', 'TINYINT', 'SMALLINT', 'FLOAT'}
    if dataset_filter_by_data_type:
        if not isinstance(dataset_filter_by_data_type, list):
            raise ValueError(
                "dataset_filter_by_data_type must be a list of strings. "
                "For single data type, use ['INTEGER'] instead of 'INTEGER'."
            )
        
        data_type_columns = dtype_info.filter(
            pl.col("type").str.to_uppercase().is_in([dt.upper() for dt in dataset_filter_by_data_type])
        )['name'].to_list()
        
        if not data_type_columns:
            raise ValueError(
                f"No columns found of types {dataset_filter_by_data_type}. "
                "Please check the data types or specify columns directly using column_list."
            )
        
        final_column_list.update(data_type_columns)
    
    final_column_list = list(final_column_list)
    
    for column in final_column_list:
        column_type = dtype_info.filter(pl.col("name") == column)['type'].item().upper()
        if not any(numeric_type in column_type for numeric_type in numeric_types):
            raise ValueError(
                f"Column '{column}' is of type {column_type}. Standard deviation requires numeric columns. "
                f"Supported types: {', '.join(sorted(numeric_types))}"
            )
    
    if filter_condition_dict:
        if not isinstance(filter_condition_dict, dict):
            raise ValueError(
                "filter_condition_dict must be a dictionary. "
                "For single filter condition, use {'column_name': value} instead of a single value."
            )
        invalid_filter_cols = list(set(filter_condition_dict.keys()) - set(dataset_columns))
        if invalid_filter_cols:
            raise ValueError(
                f"These columns were not found in your dataset: {', '.join(invalid_filter_cols)}. "
                "Please verify the column names in your filter conditions."
            )
            
        where_clause = "WHERE " + " AND ".join(
            f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}"
            for col, val in filter_condition_dict.items()
        )
    else:
        where_clause = ""
    
    sql_statements = [
        f"""
        SELECT 
            '{column}' as column_name,
            ROUND(STDDEV_POP({column}::DOUBLE), {decimal_places}) as stddev_value
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
        pl.col('stddev_value').cast(pl.Float64)
    ]).to_dicts()
    
    for result in results:
        result.update({
            'table_name': source_table,
            'execution_timestamp_utc': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            'filter_conditions': filter_condition_dict if filter_condition_dict else None,
            'filtered_by_data_type': dataset_filter_by_data_type if dataset_filter_by_data_type else None
        })
    
    return results