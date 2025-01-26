from typing import List, Dict, Union, Optional, Any
import duckdb
import polars as pl
import uuid
from datetime import datetime, timezone
from duckdb import DuckDBPyConnection
from enum import Enum

class NullBehavior(Enum):
    FAIL = "fail"
    EMPTY_STRING = "empty_string"
    IGNORE = "ignore"

def MinLength(dataset: Any,
             column_list: Optional[List[str]] = None,
             filter_condition_dict: Optional[Dict[str, Union[str, int, float]]] = None,
             dataset_filter_by_data_type: Optional[List[str]] = None,
             duckdb_connection: Optional[DuckDBPyConnection] = None,
             null_behavior: NullBehavior = NullBehavior.IGNORE
            ) -> List[Dict[str, Union[str, int, float, dict, None]]]:
    """
    Calculate minimum string length for specified columns using DuckDB.
    """
    unique_id = str(uuid.uuid4()).replace('-', '_')
    timestamp = datetime.now(timezone.utc)
    temp_table_name = f"minlength_{unique_id}"
    
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
    
    # Filter for string/varchar columns only
    string_types = ['VARCHAR', 'STRING', 'TEXT', 'CHAR', 'CHARACTER VARYING']
    string_columns = dtype_info.filter(
        pl.col("type").str.to_uppercase().is_in([t.upper() for t in string_types])
    )['name'].to_list()
    
    final_column_list = set()
    
    if column_list:
        if not isinstance(column_list, list):
            raise ValueError("column_list must be a list of strings")
        invalid_cols = set(column_list) - set(dataset_columns)
        if invalid_cols:
            raise ValueError(f"Columns not found: {', '.join(invalid_cols)}")
        # Only include string columns from column_list
        final_column_list.update(set(column_list) & set(string_columns))
    
    if dataset_filter_by_data_type:
        if not isinstance(dataset_filter_by_data_type, list):
            raise ValueError("dataset_filter_by_data_type must be a list")
        
        upper_types = [dt.upper() for dt in dataset_filter_by_data_type]
        # Add any string types that were specifically requested
        if any(t in upper_types for t in [t.upper() for t in string_types]):
            data_type_columns = dtype_info.filter(
                pl.col("type").str.to_uppercase().is_in(upper_types)
            )['name'].to_list()
            
            if not data_type_columns:
                raise ValueError(
                    f"No columns found of types {dataset_filter_by_data_type}"
                )
            
            final_column_list.update(set(data_type_columns) & set(string_columns))
    
    final_column_list = list(final_column_list)
    
    if not final_column_list:
        return []  # Return empty list if no string columns to process
    
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
    
    # Generate SQL based on null behavior
    length_expressions = {
        NullBehavior.IGNORE: "LENGTH({column})",
        NullBehavior.EMPTY_STRING: "COALESCE(LENGTH({column}), 0)",
        NullBehavior.FAIL: """
            CASE 
                WHEN COUNT(*) FILTER (WHERE {column} IS NULL) > 0 
                THEN error('NULL values not allowed') 
                ELSE MIN(LENGTH({column})) 
            END
        """
    }
    
    length_expr = length_expressions[null_behavior]
    
    # For FAIL behavior, we need to check for NULLs first
    if null_behavior == NullBehavior.FAIL:
        for column in final_column_list:
            check_nulls = f"""
                SELECT COUNT(*) as null_count
                FROM {source_table}
                {where_clause}
                WHERE {column} IS NULL
            """
            null_count = con.sql(check_nulls).fetchone()[0]
            if null_count > 0:
                raise ValueError("NULL values found when null_behavior is set to FAIL")
    
    sql_statements = [
        f"""
        SELECT 
            '{column}' as column_name,
            MIN({length_expr.format(column=column)}) as min_length
        FROM {source_table}
        {where_clause}
        """
        for column in final_column_list
    ]
    
    sql_query = " UNION ALL ".join(sql_statements)
    
    try:
        result = con.sql(sql_query).pl()
    except Exception as e:
        raise e
    
    if duckdb_connection is None:
        con.close()
    
    results = result.select([
        pl.col('column_name'),
        pl.col('min_length').cast(pl.Int64)
    ]).to_dicts()
    
    for result in results:
        result.update({
            'table_name': source_table,
            'execution_timestamp_utc': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            'filter_conditions': filter_condition_dict if filter_condition_dict else None,
            'filtered_by_data_type': dataset_filter_by_data_type if dataset_filter_by_data_type else None,
            'null_behavior': null_behavior.value
        })
    
    return results