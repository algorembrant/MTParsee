import pandas as pd

# List of the files to process
file_names = ['7_layer_output.csv']
output_file = '8_layer_output.csv'

# List to hold the extracted dataframes
extracted_data = []

for file in file_names:
    try:
        # usecols=lambda x: 'rolling' in x
        # This checks the header (first row) and extracts only columns with 'rolling' in the name
        df = pd.read_csv(file, usecols=lambda x: 'rolling' in x)
        
        # Add the filename as a prefix to the columns to keep them distinct
        # e.g., 'column_name' becomes '4_layer_output_column_name'
        prefix = file.replace('.csv', '') + '_'
        df = df.add_prefix(prefix)
        
        extracted_data.append(df)
        print(f"Processed {file}: Extracted {df.shape[1]} columns.")
        
    except FileNotFoundError:
        print(f"Error: The file '{file}' was not found.")
    except Exception as e:
        print(f"An error occurred while reading '{file}': {e}")

# Check if we have data to combine
if extracted_data:
    # Concatenate the dataframes side-by-side (axis=1)
    # This assumes the rows in both files correspond to the same samples/timestamps
    combined_df = pd.concat(extracted_data, axis=1)
    
    # Save the result to a new CSV file
    combined_df.to_csv(output_file, index=False)
    print(f"\nSuccess! Combined data saved to '{output_file}'")
else:
    print("\nNo columns were extracted.")