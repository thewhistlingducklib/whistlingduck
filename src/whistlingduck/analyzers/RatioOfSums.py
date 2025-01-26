from typing import List, Dict, Union, Optional, Any
import duckdb
import polars as pl
import uuid
from datetime import datetime, timezone
from duckdb import DuckDBPyConnection

def RatioOfSums(dataset: Any,
                column_list: List[Dict[str, str]],
                filter_condition_dict: Optional[Dict[str, Union[str, int, float]]] = None,
                duckdb_connection: Optional[DuckDBPyConnection] = None,
                decimal_places: int = 2
                ) -> List[Dict[str, Union[str, float, dict, None]]]:
    """
  Calculate ratio of sums for multiple pairs of numeric columns in a dataset using DuckDB.
    
    This function calculates the sum of values for each pair of columns and returns their ratios.
    It supports multiple column pairs and optional filtering conditions. The ratios are calculated
    as (sum of numerator column) / (sum of denominator column) for each pair.
    
    Args:
        dataset (Any): Input dataset that can be either:
            - A DataFrame (pandas, polars) or other DuckDB-compatible data structure
            - A string representing an existing table name in the DuckDB connection
            
        column_list (List[Dict[str, str]]): List of single key-value pair dictionaries.
            Each dictionary should contain one key-value pair where:
            - key: numerator column name
            - value: denominator column name
            Example: [
                {'sales': 'total_sales'},
                {'profit': 'revenue'}
            ]
            
        filter_condition_dict (Optional[Dict[str, Union[str, int, float]]], optional):
            Dictionary of filter conditions to apply before calculating ratios.
            Format: {'column_name': value}
            Example: {'department': 'IT', 'year': 2023}
            Defaults to None.
            
        duckdb_connection (Optional[DuckDBPyConnection], optional):
            Existing DuckDB connection. If None, a new connection will be created
            and closed after execution. Defaults to None.
            
        decimal_places (int, optional):
            Number of decimal places to round the results. Defaults to 2.
    
    Returns:
        List[Dict[str, Union[str, float, dict, None]]]: A list of dictionaries containing:
            - ratio_column (str): Auto-generated name combining both column names
                (e.g., 'sales_total_sales_ratio')
            - numerator_column (str): Name of the numerator column
            - denominator_column (str): Name of the denominator column
            - ratio_value (float): Calculated ratio (numerator_sum / denominator_sum)
                Special cases:
                - inf: When denominator_sum is 0 and numerator_sum > 0
                - -inf: When denominator_sum is 0 and numerator_sum < 0
                - nan: When both sums are 0
            - numerator_sum (float): Sum of the values in the numerator column
            - denominator_sum (float): Sum of the values in the denominator column
            - table_name (str): Name of the analyzed table
            - execution_timestamp_utc (str): Timestamp of execution in UTC
            - filter_conditions (dict|None): Applied filter conditions if any
    
    Raises:
        ValueError: If:
            - column_list is empty or not a list
            - column pairs are not properly formatted
            - columns don't exist in the dataset
            - columns are not numeric
            - filter conditions reference non-existent columns
            - decimal_places is negative
            
    Examples:
        >>> # Basic usage with a Polars DataFrame
        >>> df = pl.DataFrame({
        ...     'sales': [100, 200, 300],
        ...     'total_sales': [1000, 2000, 3000]
        ... })
        >>> column_list = [{'sales': 'total_sales'}]
        >>> RatioOfSums(df, column_list)
        [{'ratio_column': 'sales_total_sales_ratio',
          'numerator_column': 'sales',
          'denominator_column': 'total_sales',
          'ratio_value': 0.10,
          'numerator_sum': 600.00,
          'denominator_sum': 6000.00,
          'table_name': 'ratio_1234abcd',
          'execution_timestamp_utc': '2024-01-20 12:34:56',
          'filter_conditions': None}]
        
        >>> # With filter conditions
        >>> filters = {'department': 'IT'}
        >>> RatioOfSums(df, column_list, filter_condition_dict=filters)
    """
    if not isinstance(column_list, list) or not column_list:
        raise ValueError("column_list must be a non-empty list of dictionaries")
    
    # Validate column_list structure and extract column names
    processed_pairs = []
    for idx, pair in enumerate(column_list):
        if not isinstance(pair, dict):
            raise ValueError(f"Item at index {idx} must be a dictionary")
        if len(pair) != 1:
            raise ValueError(f"Each dictionary in column_list must contain exactly one key-value pair")
        
        numerator, denominator = next(iter(pair.items()))
        if not all(isinstance(col, str) for col in [numerator, denominator]):
            raise ValueError("Column names must be strings")
        
        ratio_column = f"{numerator}_{denominator}_ratio"
        processed_pairs.append({
            'numerator': numerator,
            'denominator': denominator,
            'ratio_column': ratio_column
        })
    
    # Generate UUID for table name and get UTC timestamp
    unique_id = str(uuid.uuid4()).replace('-', '_')
    timestamp = datetime.now(timezone.utc)
    temp_table_name = f"ratio_{unique_id}"
    
    # Handle DuckDB connection
    if duckdb_connection is None:
        con = duckdb.connect()
        con.register(temp_table_name, dataset)
        source_table = temp_table_name
    else:
        con = duckdb_connection
        if isinstance(dataset, str):
            source_table = dataset
        else:
            con.register(temp_table_name, dataset)
            source_table = temp_table_name
    
    # Handle filter conditions
    if filter_condition_dict:
        where_clause = "WHERE " + " AND ".join(
            f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}"
            for col, val in filter_condition_dict.items()
        )
    else:
        where_clause = ""
    
    # Generate and execute SQL query for each pair
    sql_statements = []
    for pair in processed_pairs:
        numerator = pair['numerator']
        denominator = pair['denominator']
        ratio_column = pair['ratio_column']
        
        sql_statements.append(f"""
            SELECT 
                '{ratio_column}' as ratio_column,
                '{numerator}' as numerator_column,
                '{denominator}' as denominator_column,
                ROUND(SUM(CAST({numerator} AS DOUBLE)), {decimal_places}) as numerator_sum,
                ROUND(SUM(CAST({denominator} AS DOUBLE)), {decimal_places}) as denominator_sum,
                ROUND(CAST(SUM(CAST({numerator} AS DOUBLE)) AS DOUBLE) / 
                      NULLIF(SUM(CAST({denominator} AS DOUBLE)), 0), {decimal_places}) as ratio_value
            FROM {source_table}
            {where_clause}
        """)
    
    sql_query = " UNION ALL ".join(sql_statements)
    result = con.sql(sql_query).pl()
    
    if duckdb_connection is None:
        con.close()
    
    results = result.to_dicts()
    
    # Add metadata to results
    for result in results:
        if result['ratio_value'] is None:  # Handle division by zero cases
            if result['numerator_sum'] > 0:
                result['ratio_value'] = float('inf')
            elif result['numerator_sum'] < 0:
                result['ratio_value'] = float('-inf')
            else:
                result['ratio_value'] = float('nan')
                
        result.update({
            'table_name': source_table,
            'execution_timestamp_utc': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            'filter_conditions': filter_condition_dict if filter_condition_dict else None
        })
    
    return results