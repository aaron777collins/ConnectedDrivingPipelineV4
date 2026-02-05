#!/usr/bin/env python3
"""
Download the full Wyoming CV BSM dataset from data.transportation.gov
Dataset: 9k4m-a3jc (~108M rows)
"""
import requests
import csv
import time
import os
from datetime import datetime

DATASET_ID = "9k4m-a3jc"
BASE_URL = f"https://data.transportation.gov/resource/{DATASET_ID}.csv"
OUTPUT_FILE = "Full_Wyoming_Data_Complete.csv"
LIMIT = 50000  # Rows per request (Socrata max is 50k for CSV)
TOTAL_ROWS = 107901297

def download_full_dataset():
    offset = 0
    first_chunk = True
    
    # Check if we have a partial download
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r') as f:
            existing_lines = sum(1 for _ in f) - 1  # subtract header
        if existing_lines > 0:
            print(f"Found existing file with {existing_lines:,} rows, resuming...")
            offset = existing_lines
            first_chunk = False
    
    print(f"Starting download from offset {offset:,}")
    print(f"Target: {TOTAL_ROWS:,} rows")
    
    start_time = time.time()
    mode = 'w' if first_chunk else 'a'
    
    with open(OUTPUT_FILE, mode, newline='') as outfile:
        while offset < TOTAL_ROWS:
            url = f"{BASE_URL}?$limit={LIMIT}&$offset={offset}&$order=:id"
            
            try:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching rows {offset:,} - {offset + LIMIT:,}...", end=" ")
                response = requests.get(url, timeout=300)
                response.raise_for_status()
                
                lines = response.text.strip().split('\n')
                
                if first_chunk:
                    # Write header
                    outfile.write(lines[0] + '\n')
                    data_lines = lines[1:]
                    first_chunk = False
                else:
                    # Skip header for subsequent chunks
                    data_lines = lines[1:] if lines[0].startswith('dataType') else lines
                
                for line in data_lines:
                    outfile.write(line + '\n')
                
                rows_written = len(data_lines)
                offset += rows_written
                
                elapsed = time.time() - start_time
                rate = offset / elapsed if elapsed > 0 else 0
                eta = (TOTAL_ROWS - offset) / rate if rate > 0 else 0
                
                print(f"OK ({rows_written:,} rows) | Total: {offset:,}/{TOTAL_ROWS:,} ({100*offset/TOTAL_ROWS:.1f}%) | ETA: {eta/3600:.1f}h")
                
                # Small delay to be nice to the API
                time.sleep(0.5)
                
            except Exception as e:
                print(f"ERROR: {e}")
                print("Waiting 30s before retry...")
                time.sleep(30)
    
    print(f"\nDownload complete! Total rows: {offset:,}")
    print(f"File: {OUTPUT_FILE}")
    print(f"Size: {os.path.getsize(OUTPUT_FILE) / (1024**3):.2f} GB")

if __name__ == "__main__":
    download_full_dataset()
