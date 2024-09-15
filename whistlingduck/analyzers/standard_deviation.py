import re
from pydantic import validate_call
from typing import Optional, List, Dict, Any

@validate_call
def standard_deviation(
    conn: Any,
    table_name: str,
    columns: Optional[List[str]] = None,
    where: Optional[str] = None,
    include_all_columns: bool = False,
    data_types: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Computes the standard deviation of  numeric column in a DuckDB table, with optional filtering.

    Args:
        conn (Any): A DuckDB connection object used to execute SQL queries.
        table_name (str): The name of the table to analyze.
        columns (Optional[List[str]], optional): A list of column names to sum. If None, all columns are considered based on data types or `include_all_columns`.
        where (Optional[str], optional): A SQL WHERE clause to filter rows before performing the sum calculation. Defaults to None.
        include_all_columns (bool, optional): If True, all columns in the table are analyzed. Defaults to False.
        data_types (Optional[List[str]], optional): A list of data types (e.g., ['INTEGER', 'DOUBLE']) to filter the columns to analyze. Defaults to None.

    Returns:
        Dict[str, Any]: A dictionary where keys are column names, and values are dictionaries containing:
            - 'total_rows' (int): The total number of rows in the table (after applying filters, if any).
            - 'sum' (float): The sum of the specified column.
            - 'filter_condition' (str or None): The SQL WHERE clause applied to filter rows, if specified.

    Raises:
        ValueError: If the table name or column names are invalid.
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
            columns_to_analyze = [
                row_name for row_name, row_type in zip(columns_df['name'].to_list(), columns_df['type'].to_list()) 
                if row_type.lower() in ['integer', 'double', 'float', 'real','bigint']
            ]
        else:
            return {}

    if not columns_to_analyze:
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
                  STDDEV_POP({col}) AS stddev,
                 {'NULL' if not escaped_where else f"'{escaped_where}'"} AS filter_condition
        FROM ({base_query}) AS base
        """
        select_statements.append(metrics_query.strip())

    final_query = " UNION ALL ".join(select_statements)
    metrics_df = conn.execute(final_query).pl()  # Fetch results directly as a Polars DataFrame

    metrics_dicts = metrics_df.to_dicts()
    metrics_dict = {
        row['column_name']: {
            'total_rows': row['total_rows'],
            'stddev': row['stddev'],
            'filter_condition': None if row['filter_condition'] == 'NULL' else row['filter_condition']
        }
        for row in metrics_dicts
    }

    return metrics_dict
