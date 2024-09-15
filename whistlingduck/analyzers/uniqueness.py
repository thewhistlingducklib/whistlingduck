import duckdb  # type: ignore
import re
from pydantic import validate_call
from typing import Optional, List, Dict, Any

@validate_call
def uniqueness(
    conn: Any,
    table_name: str,
    columns: Optional[List[str]] = None,
    where: Optional[str] = None,
    include_all_columns: bool = False,
    data_types: Optional[List[str]] = None
) -> Dict[str, Any]:
    
    """ 
    Computes the uniqueness metrics for specified columns in a DuckDB table, with optional filtering and data type selection.

    Args:
        conn (Any): A DuckDB connection object used to execute SQL queries.
        table_name (str): The name of the table to analyze.
        columns (Optional[List[str]], optional): A list of column names to analyze for uniqueness. If None, all columns are considered based on data types or `include_all_columns`.
        where (Optional[str], optional): A SQL WHERE clause to filter rows before performing the uniqueness analysis. Defaults to None.
        include_all_columns (bool, optional): If True, all columns in the table are analyzed. Defaults to False.
        data_types (Optional[List[str]], optional): A list of data types (e.g., ['INTEGER', 'TEXT']) to filter the columns to analyze. Defaults to None.

    Returns:
        Dict[str, Any]: A dictionary where keys are column names, and values are dictionaries containing the following keys:
            - 'total_rows' (int): The total number of rows in the table (after applying filters, if any).
            - 'unique_rows' (int): The number of unique rows for the column.
            - 'uniqueness_ratio' (float): The ratio of unique rows to total rows.
            - 'uniqueness_percentage' (float): The percentage of unique rows.
            - 'filter' (str or None): The SQL WHERE clause applied to filter rows, if specified.

    Raises:
        pydantic.error_wrappers.ValidationError: If the input arguments do not match the specified types.
        duckdb.Error: If there is an error executing DuckDB commands.

    """


    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', table_name):
        raise ValueError(f"Invalid table name: {table_name}")

    table_name_quoted = f'"{table_name}"'
    base_query = f"SELECT * FROM {table_name_quoted}"
    
    if where:
        base_query += f" WHERE {where}"

    table_info_query = f"PRAGMA table_info('{table_name}')"
    columns_df = conn.execute(table_info_query).pl()

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

    total_rows_query = f"SELECT COUNT(*) AS total_rows FROM ({base_query}) AS base"
    total_rows = conn.execute(total_rows_query).fetchone()[0]

    if total_rows == 0:
        return {}

    select_statements = []
    escaped_where = where.replace("'", "''") if where else None
    for col in columns_to_analyze:
        metrics_query = f"""
            SELECT
                '{col}' AS column_name,
                {total_rows} AS total_rows,
                COUNT(DISTINCT "{col}") AS unique_rows,
                ROUND(COUNT(DISTINCT "{col}") * 1.0 / {total_rows}, 2) AS uniqueness_ratio,
                ROUND(COUNT(DISTINCT "{col}") * 100.0 / {total_rows}, 2) AS uniqueness_percentage,
                {'NULL' if not escaped_where else f"'{escaped_where}'"} AS filter
            FROM ({base_query}) AS base
        """
        select_statements.append(metrics_query.strip())

    final_query = " UNION ALL ".join(select_statements)
    metrics_df = conn.execute(final_query).pl()

    metrics_dicts = metrics_df.to_dicts()
    metrics_dict = {
        row['column_name']: {
            'total_rows': row['total_rows'],
            'unique_rows': row['unique_rows'],
            'uniqueness_ratio': row['uniqueness_ratio'],
            'uniqueness_percentage': row['uniqueness_percentage'],
            'filter': None if row['filter'] == 'NULL' else row['filter']
        }
        for row in metrics_dicts
    }

    return metrics_dict
