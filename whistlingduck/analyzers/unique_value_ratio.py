import re
from pydantic import validate_call
from typing import Optional, List, Dict, Any

@validate_call
def unique_value_ratio(
    conn: Any,
    table_name: str,
    columns: Optional[List[str]] = None,
    where: Optional[str] = None,
    include_all_columns: bool = False,
    data_types: Optional[List[str]] = None
) -> Dict[str, Any]:
    """ 
    Computes the ratio of unique values to distinct values for specified columns in a DuckDB table, with optional filtering.

    Args:
        conn (Any): A DuckDB connection object used to execute SQL queries.
        table_name (str): The name of the table to analyze.
        columns (Optional[List[str]], optional): A list of column names to analyze. If None, all columns are considered based on data types or `include_all_columns`.
        where (Optional[str], optional): A SQL WHERE clause to filter rows before performing the analysis. Defaults to None.
        include_all_columns (bool, optional): If True, all columns in the table are analyzed. Defaults to False.
        data_types (Optional[List[str]], optional): A list of data types (e.g., ['INTEGER', 'TEXT']) to filter the columns to analyze. Defaults to None.

    Returns:
        Dict[str, Any]: A dictionary where keys are column names, and values are dictionaries containing:
            - 'unique_value_count' (int): The number of unique values in the column.
            - 'distinct_value_count' (int): The number of distinct values in the column.
            - 'unique_to_distinct_ratio' (float): The ratio of unique values to distinct values.
            - 'filter_condition' (str or None): The SQL WHERE clause applied to filter rows, if specified.

    Raises:
        pydantic.error_wrappers.ValidationError: If the input arguments do not match the specified types.
        duckdb.Error: If there is an error executing DuckDB commands.
    """

    # Validate the table name
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', table_name):
        raise ValueError(f"Invalid table name: {table_name}")

    table_name_quoted = f'"{table_name}"'
    base_query = f"SELECT * FROM {table_name_quoted}"
    
    if where:
        base_query += f" WHERE {where}"

    # Get table column information using DuckDB's Polars integration
    table_info_query = f"PRAGMA table_info('{table_name}')"
    columns_df = conn.execute(table_info_query).pl()

    # Determine columns to analyze
    if columns:
        for col in columns:
            if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', col):
                raise ValueError(f"Invalid column name: {col}")
        columns_to_analyze = columns
    else:
        if data_types:
            data_types_set = set(dt.lower() for dt in data_types)
            columns_to_analyze = [
                row_name for row_name, row_type in zip(columns_df['name'].to_list(), columns_df['type'].to_list()) 
                if row_type.lower() in data_types_set
            ]
        elif include_all_columns:
            columns_to_analyze = columns_df['name'].to_list()
        else:
            return {}

    # Prepare the select statements for each column
    select_statements = []
    escaped_where = where.replace("'", "''") if where else None
    for col in columns_to_analyze:
        metrics_query = f"""
            SELECT
                '{col}' AS column_name,
                COUNT(*) AS distinct_value_count,
                SUM(CASE WHEN counts.value_count = 1 THEN 1 ELSE 0 END) AS unique_value_count,
                ROUND(SUM(CASE WHEN counts.value_count = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 2) AS unique_to_distinct_ratio,
                {'NULL' if not escaped_where else f"'{escaped_where}'"} AS filter_condition
            FROM (
                SELECT
                    "{col}",
                    COUNT("{col}") AS value_count
                FROM ({base_query}) AS base
                GROUP BY "{col}"
            ) AS counts
        """
        select_statements.append(metrics_query.strip())

    # Combine all the column queries using UNION ALL
    final_query = " UNION ALL ".join(select_statements)
    metrics_df = conn.execute(final_query).pl()  # Fetch results directly as a Polars DataFrame

    # Convert the results to a dictionary
    metrics_dicts = metrics_df.to_dicts()
    metrics_dict = {
        row['column_name']: {
            'unique_value_count': row['unique_value_count'],
            'distinct_value_count': row['distinct_value_count'],
            'unique_to_distinct_ratio': row['unique_to_distinct_ratio'],
            'filter_condition': None if row['filter_condition'] == 'NULL' else row['filter_condition']
        }
        for row in metrics_dicts
    }

    return metrics_dict
