#!/usr/bin/env python3
"""
Quick validation script for profile_dask_bottlenecks.py
Runs profiling on small datasets to verify functionality.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.profile_dask_bottlenecks import DaskProfiler


def main():
    """Run quick profiling validation on small datasets."""
    print("\n" + "="*80)
    print("VALIDATION: Dask Bottleneck Profiling Script")
    print("="*80)
    print("\nRunning profiling on small datasets (1K, 5K rows) to verify functionality...")
    print("This should complete in <2 minutes.\n")

    # Use very small datasets for validation
    dataset_sizes = [1_000, 5_000]

    with DaskProfiler(dataset_sizes=dataset_sizes) as profiler:
        profiler.run_full_profile()

        # Generate report
        report_path = "/tmp/dask_profiling_validation_report.md"
        profiler.generate_report(output_path=report_path)

    print("\n" + "="*80)
    print("VALIDATION COMPLETE")
    print("="*80)
    print(f"\n✅ Profiling script validated successfully")
    print(f"✅ Report generated: {report_path}")
    print(f"\nThe full profiling script is ready to use with larger datasets.")
    print(f"Run: python3 scripts/profile_dask_bottlenecks.py")
    print()


if __name__ == "__main__":
    main()
