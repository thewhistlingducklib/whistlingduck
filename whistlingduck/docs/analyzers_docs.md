# WhistlingDuck Analyzer Function Reference

## Function Index 
| Function | User Summary | Dev Summary
| --- | --- | --- |
| [uniqueness](#uniqueness) | Computes number of rows, the number of unique values in each column, and the ratio and percentage of unique values to the total rows for the specified columns in a DuckDB table. It allows you to either specify the columns to analyze or use all columns in the table, optionally filtering them based on data types. Additionally, you can apply a WHERE clause to the table before per forming analysis. A dictionary where each key is a column name and each value is a nested dictionary containing total_rows, unique_rows, uniqueness_ratio, uniqueness_percentage & filter  | The uniqueness function, built with DuckDB and Pydantic, computes uniqueness metrics for table columns. It first retrieves column metadata using a PRAGMA table_info query, allowing selection based on column names or data types. The function dynamically builds a SQL query to include an optional WHERE clause for row filtering and computes the total row count. It then constructs SELECT statements for each column to calculate unique values, combining them using UNION ALL for efficient execution. The results are returned in a dictionary format.
| [unique_value_ratio](#unique_value_ratio) | This function calculates the ratio of unique values to distinct values in specific columns of a DuckDB table. Unique values appear only once, while distinct values are all different entries in a column. A ratio close to 1 means most values are unique, indicating high variety. A lower ratio suggests many repeated values. You can also filter the data to focus on specific rows.| The `unique_value_ratio` function, built with DuckDB,  and Pydantic, computes the ratio of unique values to distinct values for specified columns in a table. It first retrieves column metadata using a `PRAGMA table_info` query and allows for column selection based on names, data types, or an option to include all columns. The function dynamically creates a SQL query, including an optional `WHERE` clause for filtering rows. For each selected column, it constructs a nested SQL query to count distinct and unique values. These queries are combined using `UNION ALL` for efficient execution, and the final results are returned as a dictionary after being processed with DuckDB's built-in Polars integration.
| [sum](#sum) | Computes the sum of specified numeric columns in a DuckDB table, with optional filtering.| 

| [standard_deviation](#standard_deviation) | Computes the standard deviation of  numeric column in a DuckDB table, with optional filtering.| 

### uniqueness
```
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

```
#### Examples

```python
table_info = wd.read_csv_into_table('survey_results_public.csv', "survey_results_public") #to read a csv file into duckdb table

```

##### uniqueness_by_column_data_type

``` python
uniqueness_by_column_data_type= uniqueness(
    conn=table_info['duckdb_connection'],
    table_name=table_info['table_name'],
    data_types=['BIGINT']
)
```

##### uniqueness_of_multi_columns

``` python
uniqueness_of_multi_columns= uniqueness(
    conn=table_info['duckdb_connection'],
    table_name=table_info['table_name'],
    columns=['ToolsTechHaveWorkedWith','LearnCodeOnline']
)
```

##### uniqueness_of_column_with_filters

``` python
uniqueness_of_column_with_filters= uniqueness(
    conn=table_info['duckdb_connection'],
    table_name=table_info['table_name'],
    columns=['ToolsTechHaveWorkedWith'],
    where = "lower(SurveyEase)='easy' and ToolsTechHaveWorkedWith is not null"
)
```

##### uniqueness_of_all_columns

``` python
uniqueness_by_all_columns = uniqueness(
    conn=table_info['duckdb_connection'],
    table_name=table_info['table_name'],
    include_all_columns = True
)
```

##### uniqueness_of_all_columns_with_filters
``` python
uniqueness_of_all_columns_with_filters = uniqueness(
    conn=table_info['duckdb_connection'],
    table_name=table_info['table_name'],
    include_all_columns = True,
        where = "lower(SurveyEase)='easy' and ToolsTechHaveWorkedWith is not null"

)



```
### unique_value_ratio

```
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
```

#### Examples

#### unique_value_ratio_by_column_data_type

```python
unique_value_ratio_by_column_data_type= unique_value_ratio(
    conn=table_info['duckdb_connection'],
    table_name=table_info['table_name'],
    data_types=['BIGINT']
)
```
#### unique_value_ratio_of_multi_columns

```python
unique_value_ratio_of_multi_columns= unique_value_ratio(
    conn=table_info['duckdb_connection'],
    table_name=table_info['table_name'],
    columns=['ToolsTechHaveWorkedWith','LearnCodeOnline']
)
```

#### unique_value_ratio_of_column_with_filters
```python

unique_value_ratio_of_column_with_filters= unique_value_ratio(
    conn=table_info['duckdb_connection'],
    table_name=table_info['table_name'],
    columns=['ToolsTechHaveWorkedWith'],
    where = "lower(SurveyEase)='easy' and ToolsTechHaveWorkedWith is not null"
)
```

#### unique_value_ratio_by_all_columns
```python

unique_value_ratio_by_all_columns = unique_value_ratio(
    conn=table_info['duckdb_connection'],
    table_name=table_info['table_name'],
    include_all_columns = True
)
```
#### unique_value_ratio_of_all_columns_with_filters

```python

unique_value_ratio_of_all_columns_with_filters = unique_value_ratio(
    conn=table_info['duckdb_connection'],
    table_name=table_info['table_name'],
    include_all_columns = True,
        where = "lower(SurveyEase)='easy' and ToolsTechHaveWorkedWith is not null"

)
```
