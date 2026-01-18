#!/usr/bin/env python3
"""
CSV Cache to Parquet Conversion Utility

This script converts existing CSV cache files to Parquet format for use with
DaskParquetCache. It preserves the directory structure and MD5-based naming
convention used by the FileCache system.

Features:
- Scans cache directory for CSV files
- Converts CSV to Parquet using Dask for memory efficiency
- Preserves directory structure and MD5 hash naming
- Supports dry-run mode for preview
- Optional cleanup of original CSV files
- Progress reporting and error handling

Usage:
    # Dry-run (preview what will be converted)
    python scripts/convert_csv_cache_to_parquet.py --dry-run

    # Convert all CSV caches to Parquet
    python scripts/convert_csv_cache_to_parquet.py

    # Convert and delete original CSV files
    python scripts/convert_csv_cache_to_parquet.py --cleanup

    # Convert specific cache model
    python scripts/convert_csv_cache_to_parquet.py --model test

    # Force overwrite existing Parquet caches
    python scripts/convert_csv_cache_to_parquet.py --force

Author: Ralph (Dask Migration Agent)
Date: 2026-01-18
"""

import os
import sys
import argparse
import shutil
from pathlib import Path
from typing import List, Tuple, Dict

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def find_csv_caches(cache_root: str, model_filter: str = None) -> List[Tuple[str, str]]:
    """
    Find all CSV cache files in the cache directory.

    Args:
        cache_root: Root cache directory path
        model_filter: Optional model name to filter (e.g., 'test', 'debug')

    Returns:
        List of tuples: [(csv_path, parquet_path), ...]
    """
    csv_files = []
    cache_path = Path(cache_root)

    if not cache_path.exists():
        print(f"ERROR: Cache directory not found: {cache_root}")
        return []

    # Iterate through model directories
    for model_dir in cache_path.iterdir():
        if not model_dir.is_dir():
            continue

        # Skip if model filter is specified and doesn't match
        if model_filter and model_dir.name != model_filter:
            continue

        # Find all CSV files in this model directory
        for csv_file in model_dir.glob("*.csv"):
            # Generate corresponding Parquet path
            # CSV: cache/model/hash.csv -> Parquet: cache/model/hash.parquet/
            parquet_path = csv_file.with_suffix('.parquet')
            csv_files.append((str(csv_file), str(parquet_path)))

    return csv_files


def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB."""
    if os.path.exists(file_path):
        return os.path.getsize(file_path) / (1024 * 1024)
    return 0.0


def get_directory_size_mb(dir_path: str) -> float:
    """Get total size of directory in MB."""
    if not os.path.exists(dir_path):
        return 0.0

    total_size = 0
    for dirpath, dirnames, filenames in os.walk(dir_path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            total_size += os.path.getsize(filepath)

    return total_size / (1024 * 1024)


def convert_csv_to_parquet(csv_path: str, parquet_path: str, force: bool = False) -> Dict[str, any]:
    """
    Convert a single CSV cache file to Parquet format.

    Args:
        csv_path: Path to source CSV file
        parquet_path: Path to destination Parquet directory
        force: If True, overwrite existing Parquet cache

    Returns:
        Dictionary with conversion statistics
    """
    import pandas as pd
    import dask.dataframe as dd

    result = {
        'csv_path': csv_path,
        'parquet_path': parquet_path,
        'success': False,
        'csv_size_mb': 0.0,
        'parquet_size_mb': 0.0,
        'compression_ratio': 0.0,
        'rows': 0,
        'error': None
    }

    try:
        # Check if Parquet already exists
        if os.path.exists(parquet_path) and not force:
            result['error'] = 'Parquet cache already exists (use --force to overwrite)'
            return result

        # Get CSV file size
        result['csv_size_mb'] = get_file_size_mb(csv_path)

        # Read CSV with Dask for memory efficiency
        # Use pandas for small files, Dask for larger files
        csv_size_bytes = os.path.getsize(csv_path)

        if csv_size_bytes < 100 * 1024 * 1024:  # < 100MB, use pandas
            df_pandas = pd.read_csv(csv_path)
            result['rows'] = len(df_pandas)

            # Convert to Dask DataFrame
            df = dd.from_pandas(df_pandas, npartitions=1)
        else:  # >= 100MB, use Dask directly
            # Auto-detect blocksize based on file size
            blocksize = min(256 * 1024 * 1024, max(64 * 1024 * 1024, csv_size_bytes // 4))
            df = dd.read_csv(csv_path, blocksize=f"{blocksize}B")
            result['rows'] = len(df)  # This triggers compute for row count

        # Write to Parquet with snappy compression
        df.to_parquet(
            parquet_path,
            engine='pyarrow',
            compression='snappy',
            write_index=False,
            overwrite=True
        )

        # Get Parquet directory size
        result['parquet_size_mb'] = get_directory_size_mb(parquet_path)

        # Calculate compression ratio
        if result['parquet_size_mb'] > 0:
            result['compression_ratio'] = result['csv_size_mb'] / result['parquet_size_mb']

        result['success'] = True

    except Exception as e:
        result['error'] = str(e)

    return result


def print_summary(results: List[Dict[str, any]], dry_run: bool = False):
    """Print conversion summary statistics."""
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]

    print("\n" + "=" * 80)
    print("CONVERSION SUMMARY")
    print("=" * 80)

    if dry_run:
        print(f"\nDRY RUN MODE - No files were converted")
        print(f"Total CSV caches found: {len(results)}")
        total_size = sum(r['csv_size_mb'] for r in results)
        print(f"Total CSV size: {total_size:.2f} MB")
        return

    print(f"\nTotal files processed: {len(results)}")
    print(f"Successful conversions: {len(successful)}")
    print(f"Failed conversions: {len(failed)}")

    if successful:
        total_csv_size = sum(r['csv_size_mb'] for r in successful)
        total_parquet_size = sum(r['parquet_size_mb'] for r in successful)
        total_rows = sum(r['rows'] for r in successful)
        avg_compression = sum(r['compression_ratio'] for r in successful) / len(successful)

        print(f"\nStorage Statistics:")
        print(f"  CSV size:           {total_csv_size:.2f} MB")
        print(f"  Parquet size:       {total_parquet_size:.2f} MB")
        print(f"  Space saved:        {total_csv_size - total_parquet_size:.2f} MB ({((total_csv_size - total_parquet_size) / total_csv_size * 100):.1f}%)")
        print(f"  Avg compression:    {avg_compression:.2f}x")
        print(f"  Total rows:         {total_rows:,}")

    if failed:
        print(f"\nFailed conversions ({len(failed)}):")
        for r in failed[:10]:  # Show first 10 failures
            print(f"  - {os.path.basename(r['csv_path'])}: {r['error']}")
        if len(failed) > 10:
            print(f"  ... and {len(failed) - 10} more")

    print("\n" + "=" * 80)


def cleanup_csv_files(results: List[Dict[str, any]], dry_run: bool = False) -> int:
    """
    Delete original CSV files after successful conversion.

    Args:
        results: List of conversion results
        dry_run: If True, only print what would be deleted

    Returns:
        Number of files deleted (or would be deleted in dry-run)
    """
    successful = [r for r in results if r['success']]
    deleted_count = 0

    if not successful:
        return 0

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Cleaning up CSV files...")

    for result in successful:
        csv_path = result['csv_path']
        if os.path.exists(csv_path):
            if dry_run:
                print(f"  Would delete: {csv_path}")
            else:
                try:
                    os.remove(csv_path)
                    deleted_count += 1
                except Exception as e:
                    print(f"  ERROR deleting {csv_path}: {e}")

    if not dry_run:
        print(f"Deleted {deleted_count} CSV files")

    return deleted_count


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Convert CSV cache files to Parquet format for Dask migration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview conversion (dry-run)
  python scripts/convert_csv_cache_to_parquet.py --dry-run

  # Convert all CSV caches
  python scripts/convert_csv_cache_to_parquet.py

  # Convert and delete original CSVs
  python scripts/convert_csv_cache_to_parquet.py --cleanup

  # Convert only 'test' model caches
  python scripts/convert_csv_cache_to_parquet.py --model test

  # Force overwrite existing Parquet caches
  python scripts/convert_csv_cache_to_parquet.py --force
        """
    )

    parser.add_argument(
        '--cache-root',
        type=str,
        default='cache',
        help='Root cache directory (default: cache)'
    )

    parser.add_argument(
        '--model',
        type=str,
        default=None,
        help='Convert only caches for specific model (e.g., test, debug)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be converted without making changes'
    )

    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Delete original CSV files after successful conversion'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Overwrite existing Parquet caches'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed progress for each file'
    )

    args = parser.parse_args()

    # Find all CSV cache files
    print(f"Scanning cache directory: {args.cache_root}")
    if args.model:
        print(f"Filter by model: {args.model}")

    csv_files = find_csv_caches(args.cache_root, args.model)

    if not csv_files:
        print("No CSV cache files found.")
        return 0

    print(f"Found {len(csv_files)} CSV cache file(s)")

    if args.dry_run:
        print("\n[DRY RUN MODE] - No files will be modified")
        total_size = 0
        for csv_path, parquet_path in csv_files:
            size_mb = get_file_size_mb(csv_path)
            total_size += size_mb
            print(f"  {os.path.basename(csv_path)}: {size_mb:.2f} MB -> {os.path.basename(parquet_path)}")

        print(f"\nTotal size: {total_size:.2f} MB")
        print(f"Expected Parquet size: ~{total_size * 0.4:.2f} MB (estimated 60% reduction)")

        if args.cleanup:
            print(f"\n[DRY RUN] Would delete {len(csv_files)} CSV files after conversion")

        return 0

    # Import Dask only when actually converting (not for dry-run)
    try:
        import dask.dataframe as dd
        from dask.distributed import Client, LocalCluster
        from Helpers.DaskSessionManager import DaskSessionManager
    except ImportError as e:
        print(f"ERROR: Failed to import Dask dependencies: {e}")
        print("Please install: pip install dask[complete] distributed pyarrow")
        return 1

    # Initialize Dask session
    try:
        print("\nInitializing Dask cluster...")
        client = DaskSessionManager.get_client()
        print(f"Dask dashboard: {client.dashboard_link}")
    except Exception as e:
        print(f"WARNING: Could not initialize DaskSessionManager: {e}")
        print("Continuing without managed Dask session...")

    # Convert files
    print(f"\nConverting {len(csv_files)} CSV cache file(s) to Parquet...")
    results = []

    for i, (csv_path, parquet_path) in enumerate(csv_files, 1):
        if args.verbose:
            print(f"\n[{i}/{len(csv_files)}] Converting {os.path.basename(csv_path)}...")

        result = convert_csv_to_parquet(csv_path, parquet_path, force=args.force)
        results.append(result)

        if args.verbose:
            if result['success']:
                print(f"  ✓ Success: {result['csv_size_mb']:.2f} MB -> {result['parquet_size_mb']:.2f} MB "
                      f"({result['compression_ratio']:.2f}x compression, {result['rows']:,} rows)")
            else:
                print(f"  ✗ Failed: {result['error']}")
        else:
            # Show simple progress indicator
            if result['success']:
                print(f"[{i}/{len(csv_files)}] ✓ {os.path.basename(csv_path)}")
            else:
                print(f"[{i}/{len(csv_files)}] ✗ {os.path.basename(csv_path)}: {result['error']}")

    # Print summary
    print_summary(results, dry_run=False)

    # Cleanup CSV files if requested
    if args.cleanup:
        cleanup_csv_files(results, dry_run=False)

    # Return exit code
    failed_count = sum(1 for r in results if not r['success'])
    return 0 if failed_count == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
