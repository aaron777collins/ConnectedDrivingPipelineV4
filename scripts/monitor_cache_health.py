#!/usr/bin/env python3
"""
Cache Health Monitor - Task 50: Optimize cache hit rates

This script provides comprehensive cache health monitoring and analysis.
It reports cache hit rates, size, and recommends optimizations.

Usage:
    python scripts/monitor_cache_health.py [--cleanup] [--max-size-gb MAX_SIZE_GB]

Options:
    --cleanup         Perform LRU eviction if cache exceeds size threshold
    --max-size-gb N   Set maximum cache size in GB (default: 100)
    --report          Generate detailed report (default: True)
    --json            Output statistics as JSON

Examples:
    # View cache health report
    python scripts/monitor_cache_health.py

    # Cleanup cache if larger than 50GB
    python scripts/monitor_cache_health.py --cleanup --max-size-gb 50

    # Get JSON statistics
    python scripts/monitor_cache_health.py --json

Target Metrics:
    - Cache hit rate: ≥85% (excellent), 70-85% (good), <70% (needs optimization)
    - Cache size: <100GB (default threshold)
    - Average entry size: depends on dataset (BSM: 10-50MB typical)

Architecture:
    - Uses CacheManager singleton for statistics
    - Reads cache_metadata.json for historical data
    - Supports cleanup via LRU eviction policy
    - Generates markdown reports for documentation
"""

import argparse
import json
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from Decorators.CacheManager import CacheManager


def format_bytes(bytes_val: int) -> str:
    """Format bytes into human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"


def get_hit_rate_status(hit_rate: float) -> str:
    """Get status indicator for hit rate."""
    if hit_rate >= 85:
        return "✅ EXCELLENT"
    elif hit_rate >= 70:
        return "⚠️  GOOD"
    else:
        return "❌ NEEDS OPTIMIZATION"


def print_report(manager: CacheManager, args):
    """Print comprehensive cache health report."""
    stats = manager.get_statistics()
    size_mb = manager.get_cache_size_mb()
    size_gb = size_mb / 1024.0

    print("\n" + "=" * 80)
    print("CACHE HEALTH MONITOR - Task 50: Optimize Cache Hit Rates")
    print("=" * 80)
    print()

    # Statistics section
    print("📊 CACHE STATISTICS:")
    print(f"   Total Hits:        {stats['total_hits']:,}")
    print(f"   Total Misses:      {stats['total_misses']:,}")
    print(f"   Hit Rate:          {stats['hit_rate_percent']:.2f}% - {get_hit_rate_status(stats['hit_rate_percent'])}")
    print(f"   Unique Entries:    {stats['unique_entries']:,}")
    print(f"   Total Operations:  {stats['total_operations']:,}")
    print()

    # Size section
    print("💾 CACHE SIZE:")
    print(f"   Total Size:        {size_gb:.2f} GB ({size_mb:.2f} MB)")
    if stats['unique_entries'] > 0:
        avg_size_mb = size_mb / stats['unique_entries']
        print(f"   Average Entry:     {avg_size_mb:.2f} MB")
    print(f"   Max Threshold:     {args.max_size_gb} GB")
    print(f"   Usage:             {(size_gb / args.max_size_gb) * 100:.1f}%")
    print()

    # Health assessment
    print("🏥 HEALTH ASSESSMENT:")

    # Hit rate assessment
    hit_rate = stats['hit_rate_percent']
    if hit_rate >= 85:
        print(f"   ✅ Cache hit rate {hit_rate:.2f}% meets target (≥85%)")
    elif hit_rate >= 70:
        print(f"   ⚠️  Cache hit rate {hit_rate:.2f}% is acceptable but below target")
        print(f"       Recommendation: Review cache invalidation patterns")
    else:
        print(f"   ❌ Cache hit rate {hit_rate:.2f}% is below target (≥85%)")
        print(f"       Recommendation: Investigate cache misses and key generation")

    # Size assessment
    if size_gb <= args.max_size_gb * 0.8:
        print(f"   ✅ Cache size {size_gb:.2f}GB is within healthy range")
    elif size_gb <= args.max_size_gb:
        print(f"   ⚠️  Cache size {size_gb:.2f}GB approaching threshold")
        print(f"       Recommendation: Consider running cleanup soon")
    else:
        print(f"   ❌ Cache size {size_gb:.2f}GB exceeds threshold")
        print(f"       Recommendation: Run cleanup with --cleanup flag")

    print()

    # Top entries
    if stats['unique_entries'] > 0:
        print("🔝 TOP 10 MOST ACCESSED CACHE ENTRIES:")
        # Sort by total accesses
        sorted_entries = sorted(
            manager.metadata.items(),
            key=lambda x: x[1].get('hits', 0) + x[1].get('misses', 0),
            reverse=True
        )[:10]

        for i, (cache_key, metadata) in enumerate(sorted_entries, 1):
            import os
            path = metadata.get('path', 'unknown')
            hits = metadata.get('hits', 0)
            misses = metadata.get('misses', 0)
            total = hits + misses
            size_bytes = metadata.get('size_bytes', 0)

            entry_hit_rate = (hits / total * 100) if total > 0 else 0
            print(f"   {i:2d}. {os.path.basename(path)[:50]:<50}")
            print(f"       Accesses: {total:>6} (hits={hits:>5}, misses={misses:>3}, hit_rate={entry_hit_rate:.1f}%)")
            print(f"       Size: {format_bytes(size_bytes)}")
        print()

    # Recommendations
    print("💡 RECOMMENDATIONS:")
    if hit_rate < 85:
        print("   • Review cache key generation for consistency")
        print("   • Check if cache invalidation is too aggressive")
        print("   • Verify cache_variables include all relevant parameters")
    if size_gb > args.max_size_gb * 0.9:
        print("   • Run cleanup: python scripts/monitor_cache_health.py --cleanup")
    if stats['unique_entries'] > 1000:
        print("   • Consider cache entry consolidation or archival")

    print("=" * 80)
    print()


def print_json(manager: CacheManager):
    """Print cache statistics as JSON."""
    stats = manager.get_statistics()
    size_mb = manager.get_cache_size_mb()

    output = {
        'statistics': stats,
        'size': {
            'total_mb': size_mb,
            'total_gb': size_mb / 1024.0,
        },
        'entries': []
    }

    # Include per-entry details
    for cache_key, metadata in manager.metadata.items():
        entry = {
            'cache_key': cache_key,
            'hits': metadata.get('hits', 0),
            'misses': metadata.get('misses', 0),
            'size_bytes': metadata.get('size_bytes', 0),
            'created': metadata.get('created'),
            'last_accessed': metadata.get('last_accessed'),
        }
        output['entries'].append(entry)

    print(json.dumps(output, indent=2))


def main():
    """Main entry point for cache health monitoring."""
    parser = argparse.ArgumentParser(
        description='Monitor cache health and optimize hit rates (Task 50)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/monitor_cache_health.py                    # View report
  python scripts/monitor_cache_health.py --cleanup          # Cleanup cache
  python scripts/monitor_cache_health.py --json             # JSON output
        """
    )

    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Perform LRU eviction if cache exceeds size threshold'
    )
    parser.add_argument(
        '--max-size-gb',
        type=float,
        default=100.0,
        help='Maximum cache size in GB before cleanup (default: 100)'
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output statistics as JSON'
    )

    args = parser.parse_args()

    # Get CacheManager instance
    manager = CacheManager.get_instance()

    # Perform cleanup if requested
    if args.cleanup:
        print(f"\n🧹 Running cache cleanup (max size: {args.max_size_gb}GB)...\n")
        manager.cleanup_cache(max_size_gb=args.max_size_gb)

    # Output results
    if args.json:
        print_json(manager)
    else:
        print_report(manager, args)

    # Exit with status code based on hit rate
    stats = manager.get_statistics()
    if stats['total_operations'] > 0:  # Only check if there are operations
        hit_rate = stats['hit_rate_percent']
        if hit_rate < 70:
            sys.exit(1)  # Error: hit rate too low
        elif hit_rate < 85:
            sys.exit(2)  # Warning: hit rate below target
        else:
            sys.exit(0)  # Success: hit rate meets target
    else:
        sys.exit(0)  # Success: no operations yet (clean slate)


if __name__ == '__main__':
    main()
