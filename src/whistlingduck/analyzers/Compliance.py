from typing import List, Dict, Union, Optional, Any
import duckdb
import polars as pl
import uuid
from datetime import datetime, timezone
from duckdb import DuckDBPyConnection

def Compliance(dataset: Any,
              predicate: str,
              column_list: Optional[List[str]] = None,
              filter_condition_dict: Optional[Dict[str, Union[str, int, float]]] = None,
              dataset_filter_by_data_type: Optional[List[str]] = None,
              duckdb_connection: Optional[DuckDBPyConnection] = None,
              decimal_places: int = 2
             ) -> List[Dict[str, Union[str, int, float, dict, None]]]:

    """
    Calculate compliance metrics for a dataset using DuckDB.
    
    This function measures the fraction of rows that comply with the given predicate.
    It supports filtering by column names and/or data types, and can apply additional
    filter conditions before calculating compliance. Window functions and subqueries 
    are not supported in predicates.
    
    Example:
        Given a predicate "age >= 18" on a dataset with ages [15, 20, 25, 16, 30]:
        
        Compliance calculation:
        - Compliant rows: 3 (20, 25, 30)
        - Total rows: 5
        - Compliance percentage = (3/5) * 100 = 60.00%
    
    Forbidden Predicate Examples:
        The following predicates are not allowed and will raise ValueError:
        - Window functions: "COUNT(*) OVER (PARTITION BY user_id) = 1"
        - Subqueries: "salary > (SELECT AVG(salary) FROM table)"
        - Rank functions: "RANK() OVER (ORDER BY salary) = 1"
        - EXISTS clauses: "EXISTS (SELECT 1 FROM table WHERE id = user_id)"
    
    Returns:
        List[Dict[str, Union[str, int, float, dict, None]]]: A list with one dictionary containing:
            - predicate (str): The compliance predicate used
            - compliant_count (int): Count of rows meeting the predicate
            - total_count (int): Total count of rows
            - compliance_percentage (float): Percentage of compliant rows
            - table_name (str): Name of the analyzed table
            - execution_timestamp_utc (str): Timestamp of execution in UTC
            - filter_conditions (dict|None): Applied filter conditions if any
            - filtered_by_data_type (list|None): Data types used for filtering if any
            
    Args:
        dataset (Any): Input dataset that can be either:
            - A DataFrame (pandas, polars) or other DuckDB-compatible data structure
            - A string representing an existing table name in the DuckDB connection
            
        predicate (str): SQL predicate to evaluate compliance (e.g., "age >= 18").
            Must not contain window functions or subqueries.
            
        column_list (Optional[List[str]], optional): List of columns used in the predicate.
            Required for validation but not for calculation. Defaults to None.
            
        filter_condition_dict (Optional[Dict[str, Union[str, int, float]]], optional):
            Dictionary of filter conditions to apply before calculating compliance.
            Format: {'column_name': value}. Supports string, integer, and float values.
            
        dataset_filter_by_data_type (Optional[List[str]], optional): 
            Data type(s) to filter columns. Can be used together with column_list.
            
        duckdb_connection (Optional[DuckDBPyConnection], optional): Existing DuckDB connection.
            If None, a new connection will be created and closed after execution.
            
        decimal_places (int, optional): Number of decimal places to round the compliance
            percentage. Defaults to 2.
    
    Raises:
        ValueError: If the predicate contains window functions or subqueries, or if any
                   of the input parameters are invalid.
"""
    if decimal_places < 0:
        raise ValueError("decimal_places must be non-negative")
    
    if not predicate:
        raise ValueError("predicate cannot be empty example 'age >= 18'")
    
    # Validate no window functions or subqueries are used
    window_keywords = ["OVER", "PARTITION BY", "ROW_NUMBER()", "RANK()", "DENSE_RANK()", "LAG()", "LEAD()"]
    subquery_keywords = ["SELECT", "IN (SELECT", "EXISTS", "WITH"]
    
    predicate_upper = predicate.upper()
    for keyword in window_keywords:
        if keyword in predicate_upper:
            raise ValueError(f"Window functions are not allowed in predicate. Found: {keyword}")
    
    for keyword in subquery_keywords:
        if keyword in predicate_upper:
            raise ValueError(f"Subqueries are not allowed in predicate. Found: {keyword}")
    
    # Generate UUID for table name and get UTC timestamp
    unique_id = str(uuid.uuid4()).replace('-', '_')
    timestamp = datetime.now(timezone.utc)
    temp_table_name = f"compliance_{unique_id}"
    
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
    
    # Validate column list if provided
    if column_list:
        if not isinstance(column_list, list):
            raise ValueError("column_list must be a list of strings")
        invalid_cols = set(column_list) - set(dataset_columns)
        if invalid_cols:
            raise ValueError(f"These columns were not found in the dataset: {', '.join(invalid_cols)}")
    
    # Handle filter conditions
    if filter_condition_dict:
        if not isinstance(filter_condition_dict, dict):
            raise ValueError("filter_condition_dict must be a dictionary")
        invalid_filter_cols = list(set(filter_condition_dict.keys()) - set(dataset_columns))
        if invalid_filter_cols:
            raise ValueError(f"We couldn't find these columns in your dataset: {', '.join(invalid_filter_cols)}")
            
        where_clause = "WHERE " + " AND ".join(
            f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}"
            for col, val in filter_condition_dict.items()
        )
    else:
        where_clause = ""
    
    # Standard query (no window functions as they're now prohibited)
    sql_query = f"""
    SELECT 
        COUNT(CASE WHEN {predicate} THEN 1 END) as compliant_count,
        COUNT(*) as total_count,
        ROUND(COUNT(CASE WHEN {predicate} THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), {decimal_places}) as compliance_percentage
    FROM {source_table}
    {where_clause}
    """
    
    try:
        result = con.sql(sql_query).pl()
    except Exception as e:
        if duckdb_connection is None:
            con.close()
        raise ValueError(f"Invalid predicate or SQL error: {str(e)}")
    
    if duckdb_connection is None:
        con.close()
    
    # Format results
    results = result.select([
        pl.col('compliant_count').cast(pl.Int64),
        pl.col('total_count').cast(pl.Int64),
        pl.col('compliance_percentage').cast(pl.Float64)
    ]).to_dicts()
    
    # Add metadata to results
    for result in results:
        result.update({
            'predicate': predicate,
            'table_name': source_table,
            'execution_timestamp_utc': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            'filter_conditions': filter_condition_dict if filter_condition_dict else None,
            'filtered_by_data_type': dataset_filter_by_data_type if dataset_filter_by_data_type else None
        })
    
    return results