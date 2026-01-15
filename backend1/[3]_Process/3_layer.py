import pandas as pd
import os

def merge_mt5_tables():
    # File paths
    orders_file = 'extracted_orders.csv'
    deals_file = 'extracted_deals.csv'
    output_file = 'merged_extracted_orders_and_deals.csv'

    if not os.path.exists(orders_file) or not os.path.exists(deals_file):
        print("Error: One or both input CSV files are missing.")
        return

    try:
        print("Loading files...")
        orders_df = pd.read_csv(orders_file)
        deals_df = pd.read_csv(deals_file)

        orders_df['Order'] = orders_df['Order'].astype(str)
        deals_df['Order'] = deals_df['Order'].astype(str)

        print("Merging tables on 'Order' ID...")
        merged_df = pd.merge(
            orders_df, 
            deals_df, 
            on='Order', 
            how='inner', 
            suffixes=('_order', '_deal')
        )

        # --- NEW CLEANING STEP ---
        # We want to drop rows that have 6 or more NaN values.
        # axis=1: total columns, minus 6 (the max allowed empty) 
        # gives us the minimum 'good' values required.
        limit = 6
        min_valid_values = len(merged_df.columns) - limit + 1
        
        # This keeps rows ONLY if they have (Total Columns - 5) valid values.
        # Effectively, it drops any row with 6 or more NaNs.
        merged_df = merged_df.dropna(thresh=min_valid_values)
        # -------------------------

        merged_df.to_csv(output_file, index=False)
        print(f"Success! Merged data saved to '{output_file}'")
        print(f"Total rows remaining after cleaning: {len(merged_df)}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    merge_mt5_tables()