import os
import pandas as pd
import json

# Function to load and process JSON data

def load_json_data(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            return data
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON: {e}")
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file_path} was not found.")

# Function to ensure directory creation

def ensure_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Function to ingest equity prices

def ingest_equity_prices(csv_file_path):
    ensure_directory(os.path.dirname(csv_file_path))
    try:
        data = pd.read_csv(csv_file_path)
        # Process data as required
        return data
    except Exception as e:
        raise RuntimeError(f"Critical error during ingestion: {e}")

# Example usage
import sys

if __name__ == '__main__':
    # Paths for files
    json_file_path = 'data/raw/equity_prices.json'  # Example JSON File Path
    csv_file_path = 'data/raw/equity_prices.csv'

    # Load JSON data
    json_data = load_json_data(json_file_path)
    print(json_data)

    # Ingest Equity Prices
    equity_prices = ingest_equity_prices(csv_file_path)
    print(equity_prices)