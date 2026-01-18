#!/usr/bin/env python3
"""
Pipeline Config Validator

Validates that generated JSON configs match parameters from original
MClassifierLargePipeline*.py scripts.

Usage:
    python3 scripts/validate_pipeline_configs.py [OPTIONS]

Options:
    --config-dir DIR       Directory with JSON configs (default: MClassifierPipelines/configs/)
    --script-dir DIR       Directory with original .py scripts (default: repo root)
    --verbose              Show detailed validation output
    --summary              Show summary report only
    --fail-fast            Stop on first validation error

Author: Ralph (AI Development Agent)
Date: 2026-01-18
Task: 30 - Validate configs cover all parameter combinations
"""

import argparse
import ast
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any


class PipelineScriptParser:
    """Parses original MClassifierLargePipeline*.py scripts to extract parameters."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def parse_script(self, script_path: str) -> Dict:
        """Parse a pipeline script and extract all parameters.

        Args:
            script_path: Path to .py script file

        Returns:
            Dictionary with all extracted parameters
        """
        with open(script_path, 'r') as f:
            content = f.read()

        params = {}

        # Extract key parameters using regex patterns
        params['distance_meters'] = self._extract_max_dist(content)
        params['center_x'] = self._extract_value(content, r'x_pos\s*=\s*([-\d.]+)')
        params['center_y'] = self._extract_value(content, r'y_pos\s*=\s*([-\d.]+)')
        params['attack_ratio'] = self._extract_value(content, r'"ConnectedDrivingAttacker\.attack_ratio":\s*([\d.]+)')
        params['seed'] = self._extract_value(content, r'"ConnectedDrivingAttacker\.SEED":\s*(\d+)')

        # Date range parameters
        params['start_day'] = self._extract_value(content, r'"CleanerWithFilterWithinRangeXYAndDay\.startday":\s*(\d+)')
        params['start_month'] = self._extract_value(content, r'"CleanerWithFilterWithinRangeXYAndDay\.startmonth":\s*(\d+)')
        params['start_year'] = self._extract_value(content, r'"CleanerWithFilterWithinRangeXYAndDay\.startyear":\s*(\d+)')
        params['end_day'] = self._extract_value(content, r'"CleanerWithFilterWithinRangeXYAndDay\.endday":\s*(\d+)')
        params['end_month'] = self._extract_value(content, r'"CleanerWithFilterWithinRangeXYAndDay\.endmonth":\s*(\d+)')
        params['end_year'] = self._extract_value(content, r'"CleanerWithFilterWithinRangeXYAndDay\.endyear":\s*(\d+)')

        # Train/test split
        params['train_test_split'] = self._extract_train_test_split(content)

        # Attack type and parameters
        params['attack_type'], params['attack_params'] = self._extract_attack_info(content)

        # Column selection
        params['columns'] = self._extract_columns(content)

        # Filter type
        params['filter_type'] = self._extract_filter_type(content)

        # Cache enabled
        params['cache_enabled'] = self._extract_cache_enabled(script_path)

        if self.verbose:
            print(f"Extracted from {Path(script_path).name}:")
            for key, value in params.items():
                print(f"  {key}: {value}")

        return params

    def _extract_value(self, content: str, pattern: str) -> Optional[Any]:
        """Extract a value using regex pattern."""
        match = re.search(pattern, content)
        if match:
            value = match.group(1)
            # Try to convert to number
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value
        return None

    def _extract_max_dist(self, content: str) -> Optional[int]:
        """Extract max_dist parameter."""
        match = re.search(r'"ConnectedDrivingLargeDataCleaner\.max_dist":\s*(\d+)', content)
        return int(match.group(1)) if match else None

    def _extract_train_test_split(self, content: str) -> Dict:
        """Extract train/test split parameters."""
        # Look for head/tail pattern (fixed size)
        head_match = re.search(r'train\s*=\s*data\.head\((\d+)\)', content)
        if head_match:
            train_size = int(head_match.group(1))
            return {
                'type': 'fixed_size',
                'train_size': train_size
            }

        # Look for train_test_split pattern (ratio)
        ratio_match = re.search(r'train_test_split\(data,\s*test_size=([\d.]+)', content)
        if ratio_match:
            test_ratio = float(ratio_match.group(1))
            return {
                'type': 'random',
                'train_ratio': 1.0 - test_ratio,
                'test_ratio': test_ratio
            }

        return {'type': 'unknown'}

    def _extract_attack_info(self, content: str) -> Tuple[Optional[str], Dict]:
        """Extract attack type and parameters."""
        attack_params = {}

        # Check for ConstOffsetPerIDWithRandDirAndDist (const_offset_per_id)
        if 'ConstOffsetPerIDWithRandDirAndDist' in content or 'ConstPosPerCarOffset' in content:
            attack_type = 'const_offset_per_id'
            # Extract distance range
            match = re.search(r'add_attacks_positional_offset_const_per_id_with_random_direction\(min_dist=(\d+),\s*max_dist=(\d+)\)', content)
            if match:
                attack_params['min_distance'] = int(match.group(1))
                attack_params['max_distance'] = int(match.group(2))
            return attack_type, attack_params

        # Check for RandOffset
        if 'RandOffset' in content or 'positional_offset_rand' in content:
            attack_type = 'rand_offset'
            match = re.search(r'add_attacks_positional_offset_rand\(min_dist=(\d+),\s*max_dist=(\d+)\)', content)
            if match:
                attack_params['min_distance'] = int(match.group(1))
                attack_params['max_distance'] = int(match.group(2))
            return attack_type, attack_params

        # Check for RandomPos/RandPosition (override_rand)
        if 'RandomPos' in content or 'RandPosition' in content or 'positional_override_rand' in content:
            attack_type = 'override_rand'
            match = re.search(r'add_attacks_positional_override_rand\(min_dist=(\d+),\s*max_dist=(\d+)\)', content)
            if not match:
                # Try to extract from filename class name
                match = re.search(r'RandomPos(\d+)To(\d+)', content)
            if not match:
                match = re.search(r'RandPosition(\d+)To(\d+)', content)
            if match:
                attack_params['min_distance'] = int(match.group(1))
                attack_params['max_distance'] = int(match.group(2))
            return attack_type, attack_params

        # Check for RandPositionSwap
        if 'RandPositionSwap' in content or 'PositionSwap' in content or 'positional_swap_rand' in content:
            attack_type = 'swap_rand'
            return attack_type, attack_params

        # Check for ConstOffset (but not ConstOffsetPerID)
        if 'ConstOffset' in content and 'PerCar' not in content and 'PerID' not in content:
            attack_type = 'const_offset'
            return attack_type, attack_params

        return None, {}

    def _extract_columns(self, content: str) -> str:
        """Extract column selection type."""
        # Check actual column list
        col_match = re.search(r'"MConnectedDrivingDataCleaner\.columns":\s*\[(.*?)\]', content, re.DOTALL)
        if col_match:
            col_str = col_match.group(1)
            # Remove comments and get actual column names
            lines = col_str.split('\n')
            cols = []
            for line in lines:
                # Remove comments
                line = re.sub(r'#.*', '', line).strip()
                if line and line not in [',', '"', "'"]:
                    # Extract column names
                    parts = [p.strip().strip(',').strip('"').strip("'") for p in line.split(',')]
                    for p in parts:
                        if p and not p.startswith('#'):
                            cols.append(p)

            # Check for specific column combinations
            has_speed = 'coreData_speed' in cols
            has_heading = 'coreData_heading' in cols
            has_elevation = 'coreData_elevation' in cols
            has_timestamps = any('month' in c or 'day' in c or 'year' in c for c in cols)
            has_id = 'coreData_id' in cols

            # Priority order: check extended first (most columns), then specific minimal types
            if has_timestamps or has_id or len(cols) >= 10:
                return 'extended_with_timestamps'
            elif has_speed and has_heading and len(cols) <= 6:
                # Only if it's a small set with speed/heading (not part of larger extended set)
                return 'minimal_xy_elev_heading_speed'
            elif len(cols) <= 4:
                # minimal: x_pos, y_pos, elevation, isAttacker
                return 'minimal_xy_elev'

        return 'extended_with_timestamps'  # default

    def _extract_filter_type(self, content: str) -> str:
        """Extract filter type."""
        if 'CleanerWithPassthroughFilter.passthrough' in content:
            return 'passthrough'
        elif 'CleanerWithFilterWithinRangeXYAndDateRange' in content:
            return 'xy_offset_position'
        elif 'CleanerWithFilterWithinRangeXYAndDay' in content:
            return 'xy_offset_position'
        elif 'CleanerWithFilterWithinRangeXY' in content:
            return 'xy_offset_position'
        return 'xy_offset_position'  # default

    def _extract_cache_enabled(self, script_path: str) -> bool:
        """Extract cache enabled from filename."""
        return 'NoCache' not in Path(script_path).name


class ConfigValidator:
    """Validates JSON configs against original script parameters."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.parser = PipelineScriptParser(verbose=verbose)

    def validate_config(self, config_path: str, script_path: str) -> Tuple[bool, List[str]]:
        """Validate a single config against its script.

        Args:
            config_path: Path to JSON config file
            script_path: Path to original .py script

        Returns:
            (is_valid, list_of_errors)
        """
        with open(config_path, 'r') as f:
            config = json.load(f)

        script_params = self.parser.parse_script(script_path)
        errors = []

        # Only validate distance/coordinates if NOT using passthrough filter
        filter_type = script_params.get('filter_type', 'xy_offset_position')
        config_filter_type = config.get('data', {}).get('filtering', {}).get('type')

        if filter_type != 'passthrough':
            # Validate distance
            if script_params.get('distance_meters'):
                config_dist = config.get('data', {}).get('filtering', {}).get('distance_meters')
                if config_dist != script_params['distance_meters']:
                    errors.append(f"Distance mismatch: config={config_dist}, script={script_params['distance_meters']}")

            # Validate coordinates
            if script_params.get('center_x') is not None:
                config_x = config.get('data', {}).get('filtering', {}).get('center_x')
                if config_x is None or abs(config_x - script_params['center_x']) > 0.0001:
                    errors.append(f"Center X mismatch: config={config_x}, script={script_params['center_x']}")

            if script_params.get('center_y') is not None:
                config_y = config.get('data', {}).get('filtering', {}).get('center_y')
                if config_y is None or abs(config_y - script_params['center_y']) > 0.0001:
                    errors.append(f"Center Y mismatch: config={config_y}, script={script_params['center_y']}")

        # Validate date range
        if script_params.get('start_day'):
            config_start_day = config.get('data', {}).get('date_range', {}).get('start_day')
            if config_start_day != script_params['start_day']:
                errors.append(f"Start day mismatch: config={config_start_day}, script={script_params['start_day']}")

        # Validate attack ratio
        if script_params.get('attack_ratio'):
            config_ratio = config.get('attacks', {}).get('attack_ratio')
            if config_ratio is None or abs(config_ratio - script_params['attack_ratio']) > 0.001:
                errors.append(f"Attack ratio mismatch: config={config_ratio}, script={script_params['attack_ratio']}")

        # Validate attack type
        if script_params.get('attack_type'):
            config_type = config.get('attacks', {}).get('type')
            if config_type != script_params['attack_type']:
                errors.append(f"Attack type mismatch: config={config_type}, script={script_params['attack_type']}")

        # Validate attack distances
        if script_params.get('attack_params', {}).get('min_distance'):
            config_min = config.get('attacks', {}).get('min_distance')
            script_min = script_params['attack_params']['min_distance']
            if config_min != script_min:
                errors.append(f"Min attack distance mismatch: config={config_min}, script={script_min}")

        if script_params.get('attack_params', {}).get('max_distance'):
            config_max = config.get('attacks', {}).get('max_distance')
            script_max = script_params['attack_params']['max_distance']
            if config_max != script_max:
                errors.append(f"Max attack distance mismatch: config={config_max}, script={script_max}")

        # Validate columns
        if script_params.get('columns'):
            config_cols = config.get('features', {}).get('columns')
            if config_cols != script_params['columns']:
                errors.append(f"Columns mismatch: config={config_cols}, script={script_params['columns']}")

        # Validate cache
        config_cache = config.get('cache', {}).get('enabled', True)
        script_cache = script_params.get('cache_enabled', True)
        if config_cache != script_cache:
            errors.append(f"Cache mismatch: config={config_cache}, script={script_cache}")

        return (len(errors) == 0, errors)

    def validate_all(self, config_dir: str, script_dir: str, fail_fast: bool = False) -> Dict:
        """Validate all configs in directory.

        Args:
            config_dir: Directory containing JSON configs
            script_dir: Directory containing original .py scripts
            fail_fast: Stop on first validation error

        Returns:
            Validation summary dictionary
        """
        results = {
            'total': 0,
            'passed': 0,
            'failed': 0,
            'errors': {}
        }

        config_files = sorted(Path(config_dir).glob('*.json'))

        # Filter out .gitkeep and README
        config_files = [f for f in config_files if f.name.endswith('.json') and not f.name.startswith('.')]

        for config_path in config_files:
            # Find matching script
            script_name = config_path.stem + '.py'
            script_path = Path(script_dir) / script_name

            if not script_path.exists():
                print(f"⚠️  Warning: No matching script for {config_path.name}")
                continue

            results['total'] += 1

            is_valid, errors = self.validate_config(str(config_path), str(script_path))

            if is_valid:
                results['passed'] += 1
                if self.verbose:
                    print(f"✅ {config_path.name}")
            else:
                results['failed'] += 1
                results['errors'][config_path.name] = errors
                print(f"❌ {config_path.name}")
                for error in errors:
                    print(f"   - {error}")

                if fail_fast:
                    break

        return results


def main():
    parser = argparse.ArgumentParser(
        description='Validate pipeline configs against original scripts'
    )
    parser.add_argument(
        '--config-dir',
        default='MClassifierPipelines/configs/',
        help='Directory with JSON configs'
    )
    parser.add_argument(
        '--script-dir',
        default='.',
        help='Directory with original .py scripts'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed validation output'
    )
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show summary report only'
    )
    parser.add_argument(
        '--fail-fast',
        action='store_true',
        help='Stop on first validation error'
    )

    args = parser.parse_args()

    # Resolve paths
    config_dir = Path(args.config_dir).resolve()
    script_dir = Path(args.script_dir).resolve()

    if not config_dir.exists():
        print(f"Error: Config directory not found: {config_dir}")
        return 1

    if not script_dir.exists():
        print(f"Error: Script directory not found: {script_dir}")
        return 1

    # Run validation
    validator = ConfigValidator(verbose=args.verbose and not args.summary)

    print(f"\n{'='*70}")
    print(f"PIPELINE CONFIG VALIDATION")
    print(f"{'='*70}")
    print(f"Config directory: {config_dir}")
    print(f"Script directory: {script_dir}")
    print(f"{'='*70}\n")

    results = validator.validate_all(
        str(config_dir),
        str(script_dir),
        fail_fast=args.fail_fast
    )

    # Print summary
    print(f"\n{'='*70}")
    print(f"VALIDATION SUMMARY")
    print(f"{'='*70}")
    print(f"Total configs validated: {results['total']}")
    print(f"✅ Passed: {results['passed']}")
    print(f"❌ Failed: {results['failed']}")

    if results['total'] > 0:
        success_rate = (results['passed'] / results['total']) * 100
        print(f"Success rate: {success_rate:.1f}%")

    print(f"{'='*70}\n")

    # Exit with error if any failures
    if results['failed'] > 0:
        print(f"❌ Validation failed with {results['failed']} errors")
        return 1
    else:
        print(f"✅ All configs validated successfully!")
        return 0


if __name__ == '__main__':
    sys.exit(main())
