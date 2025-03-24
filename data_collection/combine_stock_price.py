import os
import pandas as pd

def combine_parquet_files(input_dir, output_file):
    parquet_files = [
        os.path.join(input_dir, file)
        for file in os.listdir(input_dir)
        if file.endswith('.parquet')
    ]
    
    if not parquet_files:
        print(f"No parquet files found in {input_dir}")
        return
    
    df_list = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            df.sort_index(inplace=True)
            df['return_forwarded'] = df['close'].pct_change().shift(-1)
            df = df.iloc[:-1]
            df_list.append(df)
            # print(f"Loaded {file} with {len(df)} rows.")
        except Exception as e:
            print(f"Error loading {file}: {e}")
    

    combined_df = pd.concat(df_list, ignore_index=False)
    combined_df.to_parquet(output_file)
    print(f"Combined data saved to {output_file}, total rows: {len(combined_df)}.")


input_directory = "data/datalake_stock_price_60min"
output_parquet = "data/data_warehouse/stock_prices.parquet"
    
combine_parquet_files(input_directory, output_parquet)