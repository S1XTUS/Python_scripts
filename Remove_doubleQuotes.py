import os
import pandas as pd

# Directory containing CSV files
csv_directory = r'E:\Kartik\Data Science\F1\raw_data_files'

# Output directory for cleaned CSV files
output_directory = r'E:\Kartik\Data Science\F1\cleaned_data_files'

# Ensure the output directory exists, create it if not
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Process each CSV file in the directory
for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        csv_file_path = os.path.join(csv_directory, filename)
        
        # Read the CSV file
        try:
            df = pd.read_csv(csv_file_path)
            print(f"File '{csv_file_path}' read successfully")
        except Exception as e:
            print(f"Error reading file '{csv_file_path}': {e}")
            continue
        
        # Remove double quotes from all fields
        df = df.applymap(lambda x: x.replace('"', '') if isinstance(x, str) else x)
        
        # Write the cleaned data to a new CSV file
        cleaned_csv_path = os.path.join(output_directory, filename)
        try:
            df.to_csv(cleaned_csv_path, index=False)
            print(f"Cleaned data saved to '{cleaned_csv_path}'")
        except Exception as e:
            print(f"Error saving cleaned data to '{cleaned_csv_path}': {e}")
