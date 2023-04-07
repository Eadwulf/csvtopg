def map_pandas_to_postgres(data_types: dict) -> dict:
    """
    Maps Pandas datatype to suitable PostgreSQL datatype.

    Args:
        data_types (dict): A dictionary containing pandas dataframe column names as its keys
            and their datatype as its values.

    Returns:
        dict: A dictionary mapping the intial keys of the data_types dictionary to their suitable
            PostgresSQL datatype.

    Example:
        >>> data_types = {'id': 'int64', 'name': 'object', 'dob': 'datetime64'}
        >>> map_pandas_to_postgres(data_types)
        {'id': 'INTEGER', 'name': 'TEXT', 'dob': 'TIMESTAMP'}
    """
    mapping = {
        'object': 'TEXT',
        'category': 'TEXT',
        'bool': 'BOOLEAN',
        'uint8': 'SMALLINT',
        'uint16': 'SMALLINT',
        'uint32': 'INTEGER',
        'uint64': 'BIGINT',
        'int8': 'SMALLINT',
        'int16': 'SMALLINT',
        'int32': 'INTEGER',
        'int64': 'BIGINT',
        'float64': 'DOUBLE PRECISION',
        'datetime64[ns]': 'TIMESTAMP',
        'timedelta[ns]': 'INTERVAL',
    }
    return {data_type: mapping[str(dtype)] for data_type, dtype in data_types.items()}
