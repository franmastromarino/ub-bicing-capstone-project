import pandas as pd

input_path = './data/other/metadata_sample_submission_2024.csv'
output_path = './data/other/metadata_sample_submission_ordered_2024.csv'


def order_dataset(input_path, output_path):
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(input_path)
    
    # Sort the DataFrame by station_id, month, day, and hour
    df_sorted = df.sort_values(by=['month', 'day', 'hour', 'station_id'])
    
    # Reset the index of the sorted DataFrame
    df_sorted.reset_index(drop=True, inplace=True)
    
    # Drop the 'index' column if it exists
    if 'index' in df_sorted.columns:
        df_sorted.drop(columns=['index'], inplace=True)
    
    # Save the sorted DataFrame to a new CSV file without the index column
    df_sorted.to_csv(output_path, index=True, index_label='index')
    
    return df_sorted

# Call the function and pass the paths to the input and output CSV files
ordered_df = order_dataset(input_path, output_path)

# Call the function and pass the paths to the input and output CSV files
ordered_df = order_dataset(input_path, output_path)

# Print the first few rows of the sorted DataFrame for verification
# print(ordered_df.head())