import pandas as pd


def get_pandas_df(filepath: str) -> pd.DataFrame:
    """
    Reads a CSV file and returns a Pandas DataFrame.

    Args:
        filepath (str): The path to the CSV file.

    Returns:
        pandas.DataFrame: The DataFrame containing the CSV data.
    """
    df = pd.read_csv(filepath)
    return df


def get_df_dtypes(df: pd.DataFrame) -> dict:
    """
    This function takes a pandas DataFrame as input and returns a dictionary with the data types of each column.

    Parameters:
    ----------
    df : pandas DataFrame
        The DataFrame to analyze

    Returns:
    -------
    dict
        A dictionary where the keys are column names and the values are the data types of the corresponding columns.
    """
    dtypes = df.dtypes
    return dict(dtypes)
