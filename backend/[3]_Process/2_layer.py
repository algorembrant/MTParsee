import pandas as pd
import numpy as np

def extract_tables_from_csv(file_path):
    """
    Extract two separate tables from a CSV file and convert to clean DataFrames
    
    Args:
        file_path (str): Path to the CSV file
    
    Returns:
        tuple: (orders_df, deals_df) - Two pandas DataFrames
    """
    # Read the entire CSV file
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Find the row indices where each table starts
    orders_start = None
    deals_start = None
    
    for i, line in enumerate(lines):
        # Check for first table header
        if 'Open Time' in line and 'Order' in line and 'Symbol' in line:
            orders_start = i
        # Check for second table header
        if 'Time' in line and 'Deal' in line and 'Direction' in line:
            deals_start = i
    
    print(f"Orders table starts at line: {orders_start}")
    print(f"Deals table starts at line: {deals_start}\n")
    
    # Extract Orders table
    if orders_start is not None:
        if deals_start is not None:
            # Read from orders_start to deals_start
            orders_df = pd.read_csv(
                file_path,
                skiprows=orders_start,
                nrows=deals_start - orders_start - 1
            )
        else:
            # Read from orders_start to end
            orders_df = pd.read_csv(file_path, skiprows=orders_start)
        
        # Clean the orders dataframe
        orders_df = clean_dataframe(orders_df)
        
        # Define expected columns for orders
        orders_columns = ['Open Time', 'Order', 'Symbol', 'Type', 'Volume', 
                         'Price', 'S / L', 'T / P', 'Time', 'State', 'Comment']
        orders_df = orders_df.reindex(columns=orders_columns, fill_value=np.nan)
        
        print("Orders Table:")
        print(orders_df.head())
        print(f"\nShape: {orders_df.shape}")
        print(f"Columns: {list(orders_df.columns)}\n")
    else:
        orders_df = None
        print("Orders table not found!\n")
    
    # Extract Deals table
    if deals_start is not None:
        # Read from deals_start to end
        deals_df = pd.read_csv(file_path, skiprows=deals_start)
        
        # Clean the deals dataframe
        deals_df = clean_dataframe(deals_df)
        
        # Define expected columns for deals
        deals_columns = ['Time', 'Deal', 'Symbol', 'Type', 'Direction', 
                        'Volume', 'Price', 'Order', 'Commission', 'Swap', 
                        'Profit', 'Balance', 'Comment']
        deals_df = deals_df.reindex(columns=deals_columns, fill_value=np.nan)
        
        print("Deals Table:")
        print(deals_df.head())
        print(f"\nShape: {deals_df.shape}")
        print(f"Columns: {list(deals_df.columns)}\n")
    else:
        deals_df = None
        print("Deals table not found!\n")
    
    return orders_df, deals_df


def clean_dataframe(df):
    """
    Clean the dataframe by removing empty rows and columns
    
    Args:
        df (DataFrame): Input dataframe
    
    Returns:
        DataFrame: Cleaned dataframe
    """
    # Remove completely empty rows
    df = df.dropna(how='all')
    
    # Remove completely empty columns
    df = df.dropna(axis=1, how='all')
    
    # Remove columns with no name (Unnamed columns that are empty)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    
    # Reset index
    df = df.reset_index(drop=True)
    
    # Strip whitespace from string columns
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip() if df[col].notna().any() else df[col]
    
    return df


def save_tables(orders_df, deals_df, output_prefix='table'):
    """
    Save the extracted tables to separate CSV files
    
    Args:
        orders_df (DataFrame): Orders table
        deals_df (DataFrame): Deals table
        output_prefix (str): Prefix for output files
    """
    if orders_df is not None:
        orders_file = f"{output_prefix}_orders.csv"
        orders_df.to_csv(orders_file, index=False)
        print(f"Orders table saved to: {orders_file}")
    
    if deals_df is not None:
        deals_file = f"{output_prefix}_deals.csv"
        deals_df.to_csv(deals_file, index=False)
        print(f"Deals table saved to: {deals_file}")


if __name__ == "__main__":
    # File path
    file_path = "Sheet1.csv"
    
    # Extract tables
    orders_df, deals_df = extract_tables_from_csv(file_path)
    
    # Save to separate files
    save_tables(orders_df, deals_df, output_prefix='extracted')
    
    # Example: Access the dataframes
    if orders_df is not None:
        print("\n" + "="*60)
        print("Orders Table Info:")
        print("="*60)
        print(orders_df.info())
        print("\nSample data:")
        print(orders_df.head(10))
    
    if deals_df is not None:
        print("\n" + "="*60)
        print("Deals Table Info:")
        print("="*60)
        print(deals_df.info())
        print("\nSample data:")
        print(deals_df.head(10))