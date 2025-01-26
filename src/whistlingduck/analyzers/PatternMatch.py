from typing import List, Dict, Union, Optional, Any
import duckdb
import polars as pl
import uuid
import re
from datetime import datetime, timezone
from duckdb import DuckDBPyConnection

def PatternMatch(dataset: Any,
                pattern: str,
                column_list: Optional[List[str]] = None,
                filter_condition_dict: Optional[Dict[str, Union[str, int, float]]] = None,
                dataset_filter_by_data_type: Optional[List[str]] = None,
                duckdb_connection: Optional[DuckDBPyConnection] = None,
                decimal_places: int = 2

               ) -> List[Dict[str, Union[str, int, float, dict, None]]]:
    """
    Calculate pattern match metrics for text columns in a dataset using DuckDB.
    
    Analyzes text columns to determine the percentage of rows matching a given regular expression pattern.
    Supports column selection by name and/or data type filtering with optional row filtering conditions.
    
    Details
    -------
    The function evaluates regex pattern matches across specified text columns, calculating match rates 
    and returning detailed statistics. Uses DuckDB for efficient pattern matching on large datasets.
    
    Example
    -------
    Consider an 'email' column:
       ["user@example.com", "invalid-email", "another@domain.com", "not-an-email", "test@test.com"]
    
    Pattern matching with email regex:
       - Matching values: "user@example.com", "another@domain.com", "test@test.com" (count = 3) 
       - Total values: 5
       - Match percentage = (3/5) * 100 = 60%
    
    Parameters
    ----------
    dataset : Any
       Input dataset (DataFrame or table name). Can be:
       - Pandas/Polars DataFrame or DuckDB-compatible structure
       - String representing existing table name in DuckDB connection
       
    pattern : str
       Regular expression pattern to match against. Must be valid regex.
    
    column_list : Optional[List[str]]
       Columns to analyze. Example: ['email', 'username']
       Used with/without dataset_filter_by_data_type. Default: None.
    
    dataset_filter_by_data_type : Optional[List[str]] 
       Filter columns by SQL type. Example: ['VARCHAR', 'TEXT']
       Used with/without column_list. Default: None.
    
    filter_condition_dict : Optional[Dict[str, Union[str, int, float]]]
       Row filter conditions. Example: {'department': 'IT'}
       Default: None.
    
    duckdb_connection : Optional[DuckDBPyConnection] 
       DuckDB connection. Creates temporary if None.
       Default: None.
    
    decimal_places : int
       Decimal places for percentages. Must be >= 0.
       Default: 2.
    
    Returns
    -------
    List[Dict[str, Union[str, int, float, dict, None]]]
       Analysis results per column:
       - column_name: Name of analyzed column
       - match_count: Number of pattern matches
       - total_count: Total rows analyzed
       - match_percentage: Percent of matching rows
       - table_name: Analyzed table name
       - execution_timestamp_utc: UTC timestamp
       - filter_conditions: Applied filters
       - filtered_by_data_type: Type filters used
       - pattern: Regex pattern used
    
    Raises
    ------
    ValueError
       - Negative decimal_places
       - Invalid regex pattern
       - No column selection method 
       - Columns not found
       - Non-string columns
       - Invalid filter columns
    """
    
    if decimal_places < 0:
        raise ValueError("decimal_places must be non-negative")

    try:
        re.compile(pattern)
    except re.error:
        raise ValueError("Invalid regular expression pattern")
        
    if column_list is None and dataset_filter_by_data_type is None:
        raise ValueError(
            "Please provide either column_list or dataset_filter_by_data_type"
        )

    unique_id = str(uuid.uuid4()).replace('-', '_')
    timestamp = datetime.now(timezone.utc)
    temp_table_name = f"pattern_match_{unique_id}"
    
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
                raise ValueError(f"Table '{dataset}' does not exist")
        else:
            try:
                con.register(temp_table_name, dataset)
                source_table = temp_table_name
            except Exception as e:
                raise ValueError(f"Failed to register dataset: {str(e)}")

    dtype_info = con.sql(f"PRAGMA table_info('{source_table}')").pl()
    dataset_columns = dtype_info['name'].to_list()
    
    final_column_list = set()
    
    if column_list:
        if not isinstance(column_list, list):
            raise ValueError(
                "column_list must be a list of strings"
            )
        invalid_cols = set(column_list) - set(dataset_columns)
        if invalid_cols:
            raise ValueError(f"Columns not found: {', '.join(invalid_cols)}")
        final_column_list.update(column_list)
    
    string_types = {'VARCHAR', 'TEXT', 'CHAR', 'STRING'}
    if dataset_filter_by_data_type:
        if not isinstance(dataset_filter_by_data_type, list):
            raise ValueError(
                "dataset_filter_by_data_type must be a list of strings"
            )
        
        data_type_columns = dtype_info.filter(
            pl.col("type").str.to_uppercase().is_in([dt.upper() for dt in dataset_filter_by_data_type])
        )['name'].to_list()
        
        if not data_type_columns:
            raise ValueError(
                f"No columns found of types {dataset_filter_by_data_type}"
            )
        
        final_column_list.update(data_type_columns)
    
    final_column_list = list(final_column_list)
    
    for column in final_column_list:
        column_type = dtype_info.filter(pl.col("name") == column)['type'].item().upper()
        if not any(string_type in column_type for string_type in string_types):
            raise ValueError(
                f"Column '{column}' must be a string type"
            )
    
    if filter_condition_dict:
        if not isinstance(filter_condition_dict, dict):
            raise ValueError("filter_condition_dict must be a dictionary")
            
        invalid_filter_cols = set(filter_condition_dict.keys()) - set(dataset_columns)
        if invalid_filter_cols:
            raise ValueError(f"Filter columns not found: {', '.join(invalid_filter_cols)}")
            
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
            COUNT(CASE WHEN regexp_matches({column}, '{pattern}') THEN 1 END) as match_count,
            COUNT(*) as total_count,
            ROUND(CAST(COUNT(CASE WHEN regexp_matches({column}, '{pattern}') THEN 1 END) AS DOUBLE) * 100.0 / 
                  NULLIF(COUNT(*), 0), {decimal_places}) as match_percentage
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
        pl.col('match_count').cast(pl.Int64),
        pl.col('total_count').cast(pl.Int64),
        pl.col('match_percentage').cast(pl.Float64)
    ]).to_dicts()
    
    for result in results:
        result.update({
            'table_name': source_table,
            'execution_timestamp_utc': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            'filter_conditions': filter_condition_dict if filter_condition_dict else None,
            'filtered_by_data_type': dataset_filter_by_data_type if dataset_filter_by_data_type else None,
            'pattern': pattern
        })
    
    return results