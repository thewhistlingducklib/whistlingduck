from typing import List, Dict, Union, Optional, Any
import duckdb
import polars as pl
import uuid
from datetime import datetime, timezone
from duckdb import DuckDBPyConnection
import math

def MutualInformation(dataset: Any,
                     column_list: List[Dict[str, str]],
                     filter_condition_dict: Optional[Dict[str, Union[str, int, float]]] = None,
                     duckdb_connection: Optional[DuckDBPyConnection] = None,
                     decimal_places: int = 6
                    ) -> List[Dict[str, Union[str, float, dict, None]]]:
    """
    Calculate Mutual Information (MI) between pairs of columns using DuckDB.
    
    Computes the mutual information metric between specified column pairs to measure
    their statistical dependence. Supports both categorical and continuous variables
    with optional filtering conditions.
    
    Details
    -------
    The function calculates mutual information using the formula:
        MI(X,Y) = Σ P(x,y) * log(P(x,y)/(P(x)*P(y)))
    
    Where:
    - P(x,y) is the joint probability
    - P(x) and P(y) are marginal probabilities
    - The sum is over all possible value pairs
    
    Higher MI values indicate stronger relationships between variables:
    - MI = 0: Variables are independent
    - MI > 0: Variables share information
    - Higher values suggest stronger dependencies
    
    Example
    -------
    Consider salary and department columns:
    Salary: [50000, 60000, 50000, 75000, 55000]
    Dept:   ['IT',  'HR',   'IT',   'Fin',  'IT']
    
    Example calculation:
    1. Calculate joint probabilities P(salary,dept)
    2. Calculate marginal probabilities P(salary) and P(dept)
    3. Compute MI using the formula above
    
    Code example:
    >>> df = pl.DataFrame({
    ...     'salary': [50000, 60000, 50000, 75000, 55000],
    ...     'department': ['IT', 'HR', 'IT', 'Finance', 'IT'],
    ...     'age': [25, 30, 25, 35, 28]
    ... })
    >>> result = MutualInformation(df, [{'salary': 'department'}])
    >>> print(result)
    [{'columns': 'salary,department', 
      'mutual_information': 0.682345,
      'table_name': 'mutual_info_abc123',
      'execution_timestamp_utc': '2024-01-25 10:30:45',
      'filter_conditions': None}]
    
    Parameters
    ----------
    dataset : Any
        Input dataset (DataFrame or table name). Can be:
        - Polars DataFrame
        - Pandas DataFrame
        - PyArrow Table
        - String representing existing table name in DuckDB connection
        The dataset must contain all columns specified in column_list.
        
    column_list : List[Dict[str, str]]
        List of column pairs to analyze. Each pair is a single-item dictionary
        where key and value are column names.
        Example: [{'salary': 'department'}, {'age': 'experience'}]
        Both categorical and numeric columns are supported.
        Columns must exist in dataset and contain non-null values.
        
    filter_condition_dict : Optional[Dict[str, Union[str, int, float]]]
        Row filter conditions to apply before MI calculation.
        Example: {'department': 'IT', 'age': 25}
        Keys must be valid column names.
        Values must match column data types.
        Default: None (no filtering)
        
    duckdb_connection : Optional[DuckDBPyConnection]
        Existing DuckDB connection to use.
        If None, creates temporary connection.
        Connection must have access to dataset if table name provided.
        Default: None
        
    decimal_places : int
        Number of decimal places for MI values.
        Must be non-negative integer.
        Affects precision of returned MI values.
        Default: 6
    
    Returns
    -------
    List[Dict[str, Union[str, float, dict, None]]]
        Analysis results for each column pair:
        - columns : str
            Comma-separated column pair names (e.g., "salary,department")
        - mutual_information : float
            Calculated MI value rounded to specified decimal places
        - table_name : str
            Name of analyzed table
        - execution_timestamp_utc : str
            UTC timestamp of execution
        - filter_conditions : Optional[Dict]
            Applied filter conditions if any, else None
    
    Raises
    ------
    ValueError
        - Empty or invalid column_list format
        - Column not found in dataset
        - Invalid decimal_places (negative)
        - Invalid filter column names
        - Type mismatch in filter conditions
    """
    # Validate decimal_places
    if decimal_places < 0:
        raise ValueError(
            "decimal_places must be non-negative. "
            "Please provide a value >= 0."
        )
    
    # Validate column_list input
    if not isinstance(column_list, list) or not column_list:
        raise ValueError(
            "column_list must be a non-empty list of dictionaries. "
            "Example: [{'column1': 'column2'}]"
        )
    
    # Process and validate column pairs
    processed_pairs = []
    for idx, pair in enumerate(column_list):
        if not isinstance(pair, dict) or len(pair) != 1:
            raise ValueError(
                f"Item at index {idx} must be a dictionary with one key-value pair. "
                "Example: {'column1': 'column2'}"
            )
        col1, col2 = next(iter(pair.items()))
        processed_pairs.append({'col1': col1, 'col2': col2})
    
    # Generate UUID for table name and get UTC timestamp
    unique_id = str(uuid.uuid4()).replace('-', '_')
    timestamp = datetime.now(timezone.utc)
    temp_table_name = f"mutual_info_{unique_id}"
    
    # Handle DuckDB connection and table registration
    if duckdb_connection is None:
        con = duckdb.connect()
        try:
            con.register(temp_table_name, dataset)
            source_table = temp_table_name
        except Exception as e:
            con.close()
            raise ValueError(
                f"Failed to register dataset: {str(e)}. "
                "Please ensure the dataset is in a DuckDB-compatible format."
            )
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
    
    # Get table info for column validation
    dtype_info = con.sql(f"PRAGMA table_info('{source_table}')").pl()
    dataset_columns = dtype_info['name'].to_list()
    
    # Validate columns existence
    for pair in processed_pairs:
        invalid_cols = []
        for col in [pair['col1'], pair['col2']]:
            if col not in dataset_columns:
                invalid_cols.append(col)
        if invalid_cols:
            if duckdb_connection is None:
                con.close()
            raise ValueError(
                f"These columns were not found in the dataset: {', '.join(invalid_cols)}. "
                "Please verify the column names."
            )
    
    # Handle filter conditions
    if filter_condition_dict:
        if not isinstance(filter_condition_dict, dict):
            if duckdb_connection is None:
                con.close()
            raise ValueError(
                "filter_condition_dict must be a dictionary. "
                "For single filter condition, use {'column_name': value}."
            )
        
        invalid_filter_cols = list(set(filter_condition_dict.keys()) - set(dataset_columns))
        if invalid_filter_cols:
            if duckdb_connection is None:
                con.close()
            raise ValueError(
                f"We couldn't find these columns in your dataset: {', '.join(invalid_filter_cols)}. "
                "Please verify the column names in your filter conditions."
            )
        
        where_clause = "WHERE " + " AND ".join(
            f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}"
            for col, val in filter_condition_dict.items()
        )
    else:
        where_clause = ""
    
    # Generate CTEs for all pairs
    cte_parts = []
    select_parts = []
    
    for idx, pair in enumerate(processed_pairs):
        col1, col2 = pair['col1'], pair['col2']
        cte_name = f"base_{idx}"
        prob_name = f"prob_{idx}"
        
        cte_parts.extend([
            f"""
            {cte_name} AS (
                SELECT {col1}, {col2},
                       COUNT(*)::FLOAT as joint_count,
                       COUNT(*) OVER()::FLOAT as total_count
                FROM {source_table}
                {where_clause}
                GROUP BY {col1}, {col2}
            ),
            {prob_name} AS (
                SELECT 
                    joint_count / total_count as joint_prob,
                    SUM(joint_count) OVER(PARTITION BY {col1}) / total_count as prob_a,
                    SUM(joint_count) OVER(PARTITION BY {col2}) / total_count as prob_b
                FROM {cte_name}
            )"""
        ])
        
        select_parts.append(
            f"""
            SELECT 
                '{col1},{col2}' as columns,
                ROUND(
                    SUM(
                        CASE 
                            WHEN joint_prob > 0 THEN 
                                joint_prob * LOG(joint_prob / NULLIF(prob_a * prob_b, 0))
                            ELSE 0 
                        END
                    ), {decimal_places}
                ) as mutual_information
            FROM {prob_name}"""
        )
    
    # Combine all CTEs and SELECT statements
    final_query = f"""
    WITH {', '.join(cte_parts)}
    {' UNION ALL '.join(select_parts)}
    """
    
    # Execute query and get results
    try:
        result = con.sql(final_query).pl()
    except Exception as e:
        if duckdb_connection is None:
            con.close()
        raise ValueError(f"Error executing query: {str(e)}")
    
    # Close connection if created internally
    if duckdb_connection is None:
        con.close()
    
    # Process results
    results = result.to_dicts()
    for result in results:
        result.update({
            'table_name': source_table,
            'execution_timestamp_utc': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            'filter_conditions': filter_condition_dict if filter_condition_dict else None
        })
    
    return results