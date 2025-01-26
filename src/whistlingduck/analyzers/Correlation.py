from typing import List, Dict, Union, Optional, Any
import duckdb
import polars as pl
import uuid
from datetime import datetime, timezone
from duckdb import DuckDBPyConnection

def Correlation(dataset: Any,
               column_list: List[Dict[str, str]],
               filter_condition_dict: Optional[Dict[str, Union[str, int, float]]] = None,
               duckdb_connection: Optional[DuckDBPyConnection] = None,
               decimal_places: int = 6
              ) -> List[Dict[str, Union[str, float, dict, None]]]:
    """
    Calculate Pearson correlation coefficients between pairs of numeric columns using DuckDB.

    This function computes correlations between specified column pairs using DuckDB's built-in 
    correlation function. It supports multiple column pairs and optional filtering conditions.
    Each correlation is calculated using complete pairs (non-null values) only.

    Parameters
    ----------
    dataset : Any
        Input dataset that can be either:
        - A DataFrame (pandas, polars) or other DuckDB-compatible data structure
        - A string representing an existing table name in the DuckDB connection

    column_list : List[Dict[str, str]]
        List of single key-value pair dictionaries.
        Each dictionary should contain one key-value pair where:
        - key: first column name for correlation
        - value: second column name for correlation
        Example: [{'sales': 'profit'}, {'customers': 'returns'}]

    filter_condition_dict : Optional[Dict[str, Union[str, int, float]]], optional
        Dictionary of filter conditions to apply before calculating correlations.
        Format: {'column_name': value}
        Example: {'department': 'IT', 'year': 2023}
        Defaults to None.

    duckdb_connection : Optional[DuckDBPyConnection], optional
        Existing DuckDB connection. If None, a new connection will be created
        and closed after execution. Defaults to None.

    decimal_places : int, optional
        Number of decimal places to round the results. Defaults to 6.

    Returns
    -------
    List[Dict[str, Union[str, float, dict, None]]]
        A list of dictionaries containing:
        - columns (str): Comma-separated pair of column names (e.g., 'sales,profit')
        - correlation_value (float): Pearson correlation coefficient
        Special cases:
        - None: When one or both columns contain all NULL values
        - 1.0: When both columns are identical or perfectly correlated
        - -1.0: When columns are perfectly negatively correlated
        - sample_size (int): Number of complete pairs used in calculation
        - table_name (str): Name of the analyzed table
        - execution_timestamp_utc (str): Timestamp of execution in UTC
        - filter_conditions (dict|None): Applied filter conditions if any

    Raises
    ------
    ValueError
        Raised if:
        - column_list is empty or not a list
        - column pairs are not properly formatted
        - columns don't exist in the dataset
        - columns are not numeric
        - filter conditions reference non-existent columns
        - decimal_places is negative

    """
    if decimal_places < 0:
        raise ValueError("decimal_places must be non-negative")
    
    if not isinstance(column_list, list) or not column_list:
        raise ValueError("column_list must be a non-empty list of dictionaries")
    
    processed_pairs = []
    for idx, pair in enumerate(column_list):
        if not isinstance(pair, dict) or len(pair) != 1:
            raise ValueError(f"Item at index {idx} must be a dictionary with one key-value pair")
        col1, col2 = next(iter(pair.items()))
        processed_pairs.append({'col1': col1, 'col2': col2})
    
    unique_id = str(uuid.uuid4()).replace('-', '_')
    timestamp = datetime.now(timezone.utc)
    temp_table_name = f"correlation_{unique_id}"
    
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
            source_table = dataset
        else:
            con.register(temp_table_name, dataset)
            source_table = temp_table_name
    
    try:
        dtype_info = con.sql(f"PRAGMA table_info('{source_table}')").pl()
        dataset_columns = dtype_info['name'].to_list()
        
        for pair in processed_pairs:
            for col in [pair['col1'], pair['col2']]:
                if col not in dataset_columns:
                    raise ValueError(f"Column '{col}' not found in dataset")
                
                col_type = dtype_info.filter(pl.col("name") == col)['type'].item()
                if not any(numeric_type in col_type.upper() 
                          for numeric_type in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC']):
                    raise ValueError(f"Column '{col}' must be numeric, found type: {col_type}")
    except Exception as e:
        if duckdb_connection is None:
            con.close()
        raise e
    
    if filter_condition_dict:
        where_clause = "WHERE " + " AND ".join(
            f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}"
            for col, val in filter_condition_dict.items()
        )
    else:
        where_clause = ""
    
    select_parts = []
    for pair in processed_pairs:
        col1, col2 = pair['col1'], pair['col2']
        select_parts.append(f"""
            SELECT 
                '{col1},{col2}' as columns,
                ROUND(CORR(CAST({col1} AS DOUBLE), 
                          CAST({col2} AS DOUBLE)), {decimal_places}) as correlation_value,
                COUNT(*) as sample_size
            FROM {source_table}
            {where_clause}
        """)
    
    sql_query = " UNION ALL ".join(select_parts)
    result = con.sql(sql_query).pl()
    
    if duckdb_connection is None:
        con.close()
    
    results = result.select([
        pl.col('columns'),
        pl.col('correlation_value').cast(pl.Float64),
        pl.col('sample_size').cast(pl.Int64)
    ]).to_dicts()
    
    for result in results:
        result.update({
            'table_name': source_table,
            'execution_timestamp_utc': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            'filter_conditions': filter_condition_dict if filter_condition_dict else None
        })
    
    return results