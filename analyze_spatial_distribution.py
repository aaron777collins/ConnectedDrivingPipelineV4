#!/usr/bin/env python3
"""
Spatial Distribution Analysis for Wyoming CV BSM Data
Calculates distances from center point to verify coverage for 200km, 100km, and 2km radii
"""

import pandas as pd
import numpy as np
from math import radians, sin, cos, sqrt, atan2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between two points on earth (in km)"""
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    # Radius of earth in kilometers
    r = 6371
    return r * c

def analyze_spatial_distribution(filepath):
    """Analyze the spatial distribution of Wyoming CV data"""
    
    logging.info(f"Starting analysis of {filepath}")
    
    # Read a sample first to determine center point
    logging.info("Reading sample data to determine center point...")
    sample_df = pd.read_csv(filepath, nrows=10000)
    
    # Extract coordinates
    sample_lats = pd.to_numeric(sample_df['coreData_position_lat'], errors='coerce')
    sample_lons = pd.to_numeric(sample_df['coreData_position_long'], errors='coerce')
    
    # Calculate center point (centroid)
    center_lat = sample_lats.median()
    center_lon = sample_lons.median()
    
    logging.info(f"Sample-based center point: ({center_lat:.6f}, {center_lon:.6f})")
    
    # Now analyze the full dataset in chunks to manage memory
    chunk_size = 100000
    total_records = 0
    radius_counts = {'2km': 0, '100km': 0, '200km': 0}
    
    logging.info("Analyzing full dataset in chunks...")
    
    for chunk_num, chunk in enumerate(pd.read_csv(filepath, chunksize=chunk_size)):
        # Extract coordinates
        lats = pd.to_numeric(chunk['coreData_position_lat'], errors='coerce')
        lons = pd.to_numeric(chunk['coreData_position_long'], errors='coerce')
        
        # Remove NaN values
        valid_mask = ~(lats.isna() | lons.isna())
        valid_lats = lats[valid_mask]
        valid_lons = lons[valid_mask]
        
        total_records += len(valid_lats)
        
        # Calculate distances for valid coordinates
        if len(valid_lats) > 0:
            distances = np.array([haversine_distance(center_lat, center_lon, lat, lon) 
                                for lat, lon in zip(valid_lats, valid_lons)])
            
            # Count records within each radius
            radius_counts['2km'] += np.sum(distances <= 2)
            radius_counts['100km'] += np.sum(distances <= 100)
            radius_counts['200km'] += np.sum(distances <= 200)
        
        # Progress update every 10 chunks
        if chunk_num % 10 == 0:
            logging.info(f"Processed chunk {chunk_num}, total valid records so far: {total_records}")
    
    # Calculate percentages
    results = {
        'center_point': {'lat': center_lat, 'lon': center_lon},
        'total_valid_records': total_records,
        'radius_analysis': {}
    }
    
    for radius, count in radius_counts.items():
        percentage = (count / total_records) * 100 if total_records > 0 else 0
        results['radius_analysis'][radius] = {
            'count': count,
            'percentage': percentage
        }
        logging.info(f"Within {radius}: {count:,} records ({percentage:.2f}%)")
    
    return results

def main():
    filepath = "April_2021_Wyoming_Data_Fixed.csv"
    
    try:
        results = analyze_spatial_distribution(filepath)
        
        print("\n" + "="*50)
        print("WYOMING CV BSM SPATIAL DISTRIBUTION ANALYSIS")
        print("="*50)
        print(f"Center Point: ({results['center_point']['lat']:.6f}, {results['center_point']['lon']:.6f})")
        print(f"Total Valid Records: {results['total_valid_records']:,}")
        print("\nRadius Analysis:")
        
        for radius, data in results['radius_analysis'].items():
            print(f"  {radius:>6}: {data['count']:>10,} records ({data['percentage']:>6.2f}%)")
        
        # Determine if sufficient data exists for experiments
        sufficient_200km = results['radius_analysis']['200km']['count'] > 1000000  # At least 1M records
        sufficient_100km = results['radius_analysis']['100km']['count'] > 500000   # At least 500K records
        sufficient_2km = results['radius_analysis']['2km']['count'] > 10000        # At least 10K records
        
        print(f"\nSufficiency Assessment:")
        print(f"  200km sufficient: {'✓ YES' if sufficient_200km else '✗ NO'} ({results['radius_analysis']['200km']['count']:,} records)")
        print(f"  100km sufficient: {'✓ YES' if sufficient_100km else '✗ NO'} ({results['radius_analysis']['100km']['count']:,} records)")
        print(f"    2km sufficient: {'✓ YES' if sufficient_2km else '✗ NO'} ({results['radius_analysis']['2km']['count']:,} records)")
        
        print("="*50)
        
    except Exception as e:
        logging.error(f"Analysis failed: {e}")
        raise

if __name__ == "__main__":
    main()