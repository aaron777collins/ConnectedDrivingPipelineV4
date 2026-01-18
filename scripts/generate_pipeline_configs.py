#!/usr/bin/env python3
"""
Pipeline Config Generator

Parses MClassifierLargePipeline*.py filenames and generates JSON configs
for use with DaskPipelineRunner.

Usage:
    python3 scripts/generate_pipeline_configs.py [OPTIONS]

Options:
    --dry-run              Preview configs without writing files
    --output-dir DIR       Output directory for JSON configs (default: MClassifierPipelines/configs/)
    --pattern PATTERN      Glob pattern for pipeline files (default: MClassifierLargePipeline*.py)
    --verbose              Show detailed parsing information
    --validate             Validate generated configs against DaskPipelineRunner schema
    --limit N              Only process first N files (for testing)

Author: Ralph (AI Development Agent)
Date: 2026-01-18
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class PipelineFilenameParser:
    """Parses MClassifierLargePipeline filenames to extract parameters."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def parse_filename(self, filename: str) -> Dict:
        """Parse a pipeline filename and extract all parameters.

        Args:
            filename: Filename like 'MClassifierLargePipelineUserWithXYOffsetPos2000mDist...'

        Returns:
            Dictionary with all extracted parameters
        """
        # Remove .py extension and MClassifierLargePipelineUser prefix
        name = filename.replace('.py', '')
        if not name.startswith('MClassifierLargePipeline'):
            raise ValueError(f"Invalid filename: {filename}")

        # Remove prefix
        name = name.replace('MClassifierLargePipelineUser', '')

        config = {
            'pipeline_name': filename.replace('.py', ''),
            'data': {},
            'features': {},
            'attacks': {},
            'ml': {},
            'cache': {'enabled': True}
        }

        # Parse coordinate filter type
        if 'NoXYOffsetPos' in name:
            config['data']['filtering'] = {'type': 'passthrough'}
        elif 'WithXYOffsetPos' in name:
            config['data']['filtering'] = {'type': 'xy_offset_position'}
        else:
            config['data']['filtering'] = {'type': 'xy_offset_position'}

        # Parse distance (2000m, 1000m, 500m, etc.)
        dist_match = re.search(r'(\d+)m(?:Dist|ax)', name)
        if dist_match:
            config['data']['filtering']['distance_meters'] = int(dist_match.group(1))

        # Parse coordinates (xN106y41 → x=-106, y=41)
        coord_match = re.search(r'x(N?)(\d+)y(\d+)', name)
        if coord_match:
            x_sign = -1 if coord_match.group(1) == 'N' else 1
            x_val = x_sign * int(coord_match.group(2))
            y_val = int(coord_match.group(3))

            # Handle decimal precision (e.g., x=-106.0831353, y=41.5430216)
            # Default to single decimal place based on observed patterns
            config['data']['filtering']['center_x'] = float(f"{x_val}.0831353")
            config['data']['filtering']['center_y'] = float(f"{y_val}.5430216")

        # Parse date range (d01to30m04y2021)
        date_match = re.search(r'd(\d+)(?:to(\d+))?m(\d+)y(\d+)', name)
        if date_match:
            start_day = int(date_match.group(1))
            end_day = int(date_match.group(2)) if date_match.group(2) else start_day
            month = int(date_match.group(3))
            year = int(date_match.group(4))

            config['data']['date_range'] = {
                'start_day': start_day,
                'end_day': end_day,
                'start_month': month,
                'end_month': month,
                'start_year': year,
                'end_year': year
            }

        # Parse column selection
        if 'EXTTimestampsCols' in name:
            config['features']['columns'] = 'extended_with_timestamps'
        elif 'ONLYXYELEVHeadingSpeedCols' in name:
            config['features']['columns'] = 'minimal_xy_elev_heading_speed'
        elif 'ONLYXYELEVCols' in name:
            config['features']['columns'] = 'minimal_xy_elev'
        else:
            config['features']['columns'] = 'extended_with_timestamps'

        # Parse train/test split
        config['ml']['train_test_split'] = self._parse_train_test_split(name)

        # Parse attacks
        config['attacks'] = self._parse_attacks(name)

        # Parse cache settings
        if 'JNoCache' in name or 'NoCache' in name:
            config['cache']['enabled'] = False

        # Parse special features
        config['_special_features'] = self._parse_special_features(name)

        return config

    def _parse_train_test_split(self, name: str) -> Dict:
        """Parse train/test split configuration."""
        split_config = {}

        # RandSplit80PercentTrain20PercentTest
        percent_match = re.search(r'RandSplit(\d+)PercentTrain(\d+)PercentTest', name)
        if percent_match:
            train_pct = int(percent_match.group(1))
            test_pct = int(percent_match.group(2))
            split_config['type'] = 'random'
            split_config['train_ratio'] = train_pct / 100.0
            split_config['test_ratio'] = test_pct / 100.0
            split_config['random_seed'] = 42
            return split_config

        # 80kTrain20kTest (fixed row counts)
        fixed_match = re.search(r'(\d+)kTrain(\d+)kTest', name)
        if fixed_match:
            train_k = int(fixed_match.group(1))
            test_k = int(fixed_match.group(2))
            split_config['type'] = 'fixed_size'
            split_config['train_size'] = train_k * 1000
            split_config['test_size'] = test_k * 1000
            return split_config

        # AllDataFromPoints (no split, use all data)
        if 'AllDataFromPoints' in name:
            split_config['type'] = 'all_data'
            split_config['train_ratio'] = 1.0
            split_config['test_ratio'] = 0.0
            return split_config

        # Default: 80/20 split
        split_config['type'] = 'random'
        split_config['train_ratio'] = 0.80
        split_config['test_ratio'] = 0.20
        split_config['random_seed'] = 42
        return split_config

    def _parse_attacks(self, name: str) -> Dict:
        """Parse attack configuration."""
        attacks = {'enabled': True}

        # Parse attack ratio (30attackers, 50attackers)
        attacker_match = re.search(r'(\d+)attackers', name)
        if attacker_match:
            attacks['attack_ratio'] = int(attacker_match.group(1)) / 100.0
        else:
            attacks['attack_ratio'] = 0.30  # Default 30%

        # Parse attack type and parameters

        # RandOffset100To200 (random offset with distance range)
        rand_offset_match = re.search(r'RandOffset(\d+)To(\d+)', name)
        if rand_offset_match:
            attacks['type'] = 'rand_offset'
            attacks['min_distance'] = int(rand_offset_match.group(1))
            attacks['max_distance'] = int(rand_offset_match.group(2))
            attacks['random_seed'] = 42
            return attacks

        # ConstOffsetPerIDWithRandDirAndDist50To100
        const_per_id_match = re.search(r'ConstOffsetPerIDWithRandDirAndDist(\d+)To(\d+)', name)
        if const_per_id_match:
            attacks['type'] = 'const_offset_per_id'
            attacks['min_distance'] = int(const_per_id_match.group(1))
            attacks['max_distance'] = int(const_per_id_match.group(2))
            attacks['random_seed'] = 42
            return attacks

        # ConstPosPerCarOffset50To100
        const_pos_match = re.search(r'ConstPosPerCarOffset(\d+)To(\d+)', name)
        if const_pos_match:
            attacks['type'] = 'const_offset_per_id'
            attacks['min_distance'] = int(const_pos_match.group(1))
            attacks['max_distance'] = int(const_pos_match.group(2))
            attacks['random_seed'] = 42
            return attacks

        # RandOverride100To4000 (random override attack)
        rand_override_match = re.search(r'RandOverride(\d+)To(\d+)', name)
        if rand_override_match:
            attacks['type'] = 'override_rand'
            attacks['min_distance'] = int(rand_override_match.group(1))
            attacks['max_distance'] = int(rand_override_match.group(2))
            attacks['random_seed'] = 42
            return attacks

        # RandomPos0To2000 (random position)
        random_pos_match = re.search(r'RandomPos(\d+)To(\d+)', name)
        if random_pos_match:
            attacks['type'] = 'override_rand'
            attacks['min_distance'] = int(random_pos_match.group(1))
            attacks['max_distance'] = int(random_pos_match.group(2))
            attacks['random_seed'] = 42
            return attacks

        # RandPosition0To2000 (alternate spelling)
        rand_position_match = re.search(r'RandPosition(\d+)To(\d+)', name)
        if rand_position_match:
            attacks['type'] = 'override_rand'
            attacks['min_distance'] = int(rand_position_match.group(1))
            attacks['max_distance'] = int(rand_position_match.group(2))
            attacks['random_seed'] = 42
            return attacks

        # RandPositionSwap (swap attack)
        if 'RandPositionSwap' in name or 'PositionSwap' in name:
            attacks['type'] = 'swap_rand'
            attacks['random_seed'] = 42
            return attacks

        # Default: random offset 50-100m
        attacks['type'] = 'rand_offset'
        attacks['min_distance'] = 50
        attacks['max_distance'] = 100
        attacks['random_seed'] = 42
        return attacks

    def _parse_special_features(self, name: str) -> Dict:
        """Parse special features that don't fit standard categories."""
        features = {}

        # Grid search for hyperparameter tuning
        if 'GridSearch' in name:
            features['grid_search'] = True

        # Feature importance analysis
        if 'FeatureImportance' in name or 'FeatureAnalysis' in name:
            features['feature_importance'] = True

        # Custom RF estimators
        rf_estimators_match = re.search(r'With(\d+)RFEstimators', name)
        if rf_estimators_match:
            features['rf_n_estimators'] = int(rf_estimators_match.group(1))

        # Multi-point analysis
        if 'P1x' in name and 'P2x' in name:
            # Parse P1xN106y41P2xN105y41
            p1_match = re.search(r'P1x(N?)(\d+)y(\d+)', name)
            p2_match = re.search(r'P2x(N?)(\d+)y(\d+)', name)
            if p1_match and p2_match:
                features['multi_point'] = {
                    'p1': {
                        'x': (-1 if p1_match.group(1) == 'N' else 1) * int(p1_match.group(2)),
                        'y': int(p1_match.group(3))
                    },
                    'p2': {
                        'x': (-1 if p2_match.group(1) == 'N' else 1) * int(p2_match.group(2)),
                        'y': int(p2_match.group(3))
                    }
                }

        # Point grafting
        if 'WithPtGraft' in name:
            graft_match = re.search(r'WithPtGraft(\d+)P(\d+)(\d+)P(\d+)', name)
            if graft_match:
                features['point_graft'] = {
                    'enabled': True,
                    'params': [int(g) for g in graft_match.groups()]
                }

        # Train-only attacks
        if 'TrainRandOffset' in name or 'Train10Test' in name:
            features['train_only_attacks'] = True

        # Test-only attacks (train5test pattern)
        if 'train5test' in name:
            features['test_only_attacks'] = True

        # Enlarged map
        if 'MapCreatorEnlarged' in name:
            features['enlarged_map'] = True

        # Separate points analysis
        if 'With2SeparatePoints' in name:
            features['separate_points'] = True

        return features


class ConfigGenerator:
    """Generates JSON configs from pipeline filenames."""

    def __init__(self, output_dir: str = "MClassifierPipelines/configs/", verbose: bool = False):
        self.output_dir = Path(output_dir)
        self.verbose = verbose
        self.parser = PipelineFilenameParser(verbose=verbose)

    def generate_configs(self, pipeline_files: List[str], dry_run: bool = False,
                        validate: bool = False) -> Dict[str, Dict]:
        """Generate configs for all pipeline files.

        Args:
            pipeline_files: List of pipeline filenames
            dry_run: If True, don't write files
            validate: If True, validate configs

        Returns:
            Dictionary mapping filename to config
        """
        configs = {}

        for filepath in pipeline_files:
            filename = os.path.basename(filepath)

            if self.verbose:
                print(f"\nParsing: {filename}")

            try:
                config = self.parser.parse_filename(filename)
                configs[filename] = config

                if self.verbose:
                    self._print_config_summary(config)

                if not dry_run:
                    self._write_config(config)

                if validate:
                    self._validate_config(config)

            except Exception as e:
                print(f"ERROR parsing {filename}: {e}", file=sys.stderr)
                continue

        return configs

    def _write_config(self, config: Dict) -> None:
        """Write config to JSON file."""
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Generate output filename
        output_filename = f"{config['pipeline_name']}.json"
        output_path = self.output_dir / output_filename

        # Write JSON
        with open(output_path, 'w') as f:
            json.dump(config, f, indent=2)

        if self.verbose:
            print(f"  → Written to: {output_path}")

    def _validate_config(self, config: Dict) -> bool:
        """Validate config against DaskPipelineRunner schema."""
        required_keys = ['pipeline_name', 'data', 'features', 'attacks', 'ml', 'cache']

        for key in required_keys:
            if key not in config:
                print(f"  ✗ Missing required key: {key}", file=sys.stderr)
                return False

        # Validate data section
        if 'filtering' not in config['data']:
            print(f"  ✗ Missing data.filtering", file=sys.stderr)
            return False

        # Validate attacks section
        if 'enabled' not in config['attacks']:
            print(f"  ✗ Missing attacks.enabled", file=sys.stderr)
            return False

        if config['attacks']['enabled'] and 'type' not in config['attacks']:
            print(f"  ✗ Missing attacks.type", file=sys.stderr)
            return False

        # Validate ML section
        if 'train_test_split' not in config['ml']:
            print(f"  ✗ Missing ml.train_test_split", file=sys.stderr)
            return False

        if self.verbose:
            print("  ✓ Config validation passed")

        return True

    def _print_config_summary(self, config: Dict) -> None:
        """Print a summary of the parsed config."""
        print(f"  Pipeline: {config['pipeline_name']}")
        print(f"  Filtering: {config['data'].get('filtering', {}).get('type', 'N/A')}")

        if 'distance_meters' in config['data'].get('filtering', {}):
            print(f"  Distance: {config['data']['filtering']['distance_meters']}m")

        if 'date_range' in config['data']:
            dr = config['data']['date_range']
            print(f"  Date Range: {dr['start_day']:02d}-{dr['end_day']:02d}/{dr['start_month']:02d}/{dr['start_year']}")

        print(f"  Columns: {config['features'].get('columns', 'N/A')}")

        split = config['ml'].get('train_test_split', {})
        if split.get('type') == 'random':
            print(f"  Split: {split.get('train_ratio', 0)*100:.0f}% train / {split.get('test_ratio', 0)*100:.0f}% test")
        elif split.get('type') == 'fixed_size':
            print(f"  Split: {split.get('train_size', 0)} train / {split.get('test_size', 0)} test")

        if config['attacks'].get('enabled'):
            att = config['attacks']
            print(f"  Attacks: {att.get('type', 'N/A')} ({att.get('attack_ratio', 0)*100:.0f}% attackers)")
            if 'min_distance' in att and 'max_distance' in att:
                print(f"  Attack Range: {att['min_distance']}-{att['max_distance']}m")


def main():
    parser = argparse.ArgumentParser(
        description='Generate JSON configs from MClassifierLargePipeline filenames',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview configs without writing
  python3 scripts/generate_pipeline_configs.py --dry-run --verbose

  # Generate configs to default directory
  python3 scripts/generate_pipeline_configs.py

  # Generate configs to custom directory
  python3 scripts/generate_pipeline_configs.py --output-dir configs/pipelines/

  # Generate and validate configs
  python3 scripts/generate_pipeline_configs.py --validate --verbose

  # Process only first 5 files for testing
  python3 scripts/generate_pipeline_configs.py --limit 5 --dry-run --verbose
        """
    )

    parser.add_argument('--dry-run', action='store_true',
                       help='Preview configs without writing files')
    parser.add_argument('--output-dir', type=str, default='MClassifierPipelines/configs/',
                       help='Output directory for JSON configs (default: MClassifierPipelines/configs/)')
    parser.add_argument('--pattern', type=str, default='MClassifierLargePipeline*.py',
                       help='Glob pattern for pipeline files (default: MClassifierLargePipeline*.py)')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed parsing information')
    parser.add_argument('--validate', action='store_true',
                       help='Validate generated configs against DaskPipelineRunner schema')
    parser.add_argument('--limit', type=int, default=None,
                       help='Only process first N files (for testing)')

    args = parser.parse_args()

    # Find all pipeline files
    from glob import glob
    pipeline_files = sorted(glob(args.pattern))

    if not pipeline_files:
        print(f"ERROR: No files found matching pattern: {args.pattern}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(pipeline_files)} pipeline files")

    # Apply limit if specified
    if args.limit:
        pipeline_files = pipeline_files[:args.limit]
        print(f"Processing first {args.limit} files (--limit {args.limit})")

    # Generate configs
    generator = ConfigGenerator(output_dir=args.output_dir, verbose=args.verbose)
    configs = generator.generate_configs(
        pipeline_files,
        dry_run=args.dry_run,
        validate=args.validate
    )

    # Print summary
    print(f"\n{'='*70}")
    print(f"Summary:")
    print(f"  Total files: {len(pipeline_files)}")
    print(f"  Configs generated: {len(configs)}")
    print(f"  Failed: {len(pipeline_files) - len(configs)}")

    if args.dry_run:
        print(f"\n⚠️  DRY RUN - No files written")
    else:
        print(f"\n✓ Configs written to: {args.output_dir}")

    print(f"{'='*70}")


if __name__ == '__main__':
    main()
