from typing import List, Dict, Union, Optional, Any
import duckdb
import polars as pl
import uuid
from datetime import datetime, timezone
from duckdb import DuckDBPyConnection

def Distinctness(dataset: Any, 
                column_list: Optional[List[str]] = None, 
                filter_condition_dict: Optional[Dict[str, Union[str, int, float]]] = None,
                dataset_filter_by_data_type: Optional[List[str]] = None,
                duckdb_connection: Optional[DuckDBPyConnection] = None,
                decimal_places: int = 2
               ) -> List[Dict[str, Union[str, float, dict, None]]]:
    
    """
    Calculate distinctness (ratio of unique values) for specified columns using DuckDB.
    
    Distinctness measures the proportion of unique values in columns, providing insights into
    data cardinality and quality. It is calculated as:
    COUNT(DISTINCT column) / COUNT(column)
    
    Example:
        Given column values ["A", "B", "A", "C", "B", "D"]:
        - Distinct values: 4 (A, B, C, D)
        - Total values: 6
        - Distinctness ratio = 4/6 ≈ 0.67
        - Distinctness percentage = 67%
    
    Args:
        dataset: Input dataset as DataFrame or table name string. For DataFrames with
            duckdb_connection, registers as temporary table.
        column_list: Optional list of column names to analyze. Can combine with
            dataset_filter_by_data_type.
        filter_condition_dict: Optional filter conditions as {'column': value} dict.
            Example: {'category': 'electronics', 'price': 100}
        dataset_filter_by_data_type: Optional data types to filter columns.
            Example: ['VARCHAR', 'INTEGER']. Can combine with column_list.
        duckdb_connection: Optional existing DuckDB connection. Creates new connection
            if None.
        decimal_places: Number of decimal places for rounding distinctness values.
    
    Returns:
        List of dicts with analysis results:
        - column_name (str): Analyzed column name
        - distinctness_ratio (float): Ratio of distinct values (0-1)
        - distinctness_percentage (float): Percentage of distinct values (0-100)
        - table_name (str): Analyzed table name
        - execution_timestamp_utc (str): UTC execution timestamp
        - filter_conditions (dict|None): Applied filters if any
        - filtered_by_data_type (list|None): Data type filters if any
    
    Raises:
        ValueError:
            - If decimal_places negative
            - If neither column_list nor dataset_filter_by_data_type provided
            - If specified columns missing from dataset
            - If no columns match specified types
            - If filter conditions reference missing columns
            - If dataset registration fails
    """
    if decimal_places < 0:
        raise ValueError("decimal_places must be non-negative")
        
    unique_id = str(uuid.uuid4()).replace('-', '_')
    timestamp = datetime.now(timezone.utc)
    temp_table_name = f"distinctness_{unique_id}"
    
    if column_list is None and dataset_filter_by_data_type is None:
        raise ValueError(
            "Please provide either column_list or dataset_filter_by_data_type."
        )
    
    # Handle DuckDB connection
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
    
    # Get table info and validate columns
    dtype_info = con.sql(f"PRAGMA table_info('{source_table}')").pl()
    dataset_columns = dtype_info['name'].to_list()
    
    final_column_list = set()
    
    if column_list:
        if not isinstance(column_list, list):
            raise ValueError("column_list must be a list of strings")
        invalid_cols = set(column_list) - set(dataset_columns)
        if invalid_cols:
            raise ValueError(f"Columns not found: {', '.join(invalid_cols)}")
        final_column_list.update(column_list)
    
    if dataset_filter_by_data_type:
        if not isinstance(dataset_filter_by_data_type, list):
            raise ValueError("dataset_filter_by_data_type must be a list")
        
        data_type_columns = dtype_info.filter(
            pl.col("type").str.to_uppercase().is_in(
                [dt.upper() for dt in dataset_filter_by_data_type]
            )
        )['name'].to_list()
        
        if not data_type_columns:
            raise ValueError(
                f"No columns found of types {dataset_filter_by_data_type}"
            )
        
        final_column_list.update(data_type_columns)
    
    final_column_list = list(final_column_list)
    
    # Handle filter conditions
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
    
    # Generate SQL for distinctness calculation
    sql_statements = [
        f"""
        SELECT 
            '{column}' as column_name,
            ROUND(COUNT(DISTINCT {column}) * 1.0 / NULLIF(COUNT(*), 0), {decimal_places}) as distinctness_ratio,
            ROUND(COUNT(DISTINCT {column}) * 100.0 / NULLIF(COUNT(*), 0), {decimal_places}) as distinctness_percentage
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
        pl.col('distinctness_ratio').cast(pl.Float64),
        pl.col('distinctness_percentage').cast(pl.Float64)
    ]).to_dicts()
    
    for result in results:
        result.update({
            'table_name': source_table,
            'execution_timestamp_utc': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            'filter_conditions': filter_condition_dict if filter_condition_dict else None,
            'filtered_by_data_type': dataset_filter_by_data_type if dataset_filter_by_data_type else None
        })
    
    return results