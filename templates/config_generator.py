#!/usr/bin/env python3
"""
Configuration Generator for Connected Driving Simulation Matrix

This module generates full pipeline configurations from template parameters.
It supports all spatial radii (200km, 100km, 2km) and feature sets 
(BASIC, BASIC_WITH_ID, MOVEMENT, MOVEMENT_WITH_ID, EXTENDED, EXTENDED_WITH_ID).
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, Any, List


class ConfigurationGenerator:
    """Generates pipeline configurations from template parameters."""
    
    def __init__(self, schema_file: str = "config_schema.json"):
        """Initialize with configuration schema."""
        self.schema_file = schema_file
        self.schema = self._load_schema()
        
    def _load_schema(self) -> Dict[str, Any]:
        """Load configuration schema."""
        try:
            with open(self.schema_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Schema file not found: {self.schema_file}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in schema file: {e}")
    
    def validate_template_config(self, template_config: Dict[str, Any]) -> List[str]:
        """Validate template configuration parameters."""
        errors = []
        
        # Check required parameters
        required_params = ['spatial_radius', 'feature_set']
        for param in required_params:
            if param not in template_config:
                errors.append(f"Missing required parameter: {param}")
        
        # Validate spatial_radius
        if 'spatial_radius' in template_config:
            valid_radii = self.schema['template_parameters']['spatial_radius']['values']
            if template_config['spatial_radius'] not in valid_radii:
                errors.append(f"Invalid spatial_radius. Must be one of: {valid_radii}")
        
        # Validate feature_set
        if 'feature_set' in template_config:
            valid_features = self.schema['template_parameters']['feature_set']['values']
            if template_config['feature_set'] not in valid_features:
                errors.append(f"Invalid feature_set. Must be one of: {valid_features}")
        
        # Validate malicious_ratio if present
        if 'malicious_ratio' in template_config:
            ratio = template_config['malicious_ratio']
            if not isinstance(ratio, (int, float)) or ratio <= 0 or ratio > 0.5:
                errors.append("malicious_ratio must be between 0.01 and 0.50")
        
        return errors
    
    def generate_pipeline_config(self, template_config_file: str) -> Dict[str, Any]:
        """Generate full pipeline configuration from template config file."""
        
        # Load template configuration
        with open(template_config_file, 'r') as f:
            template_data = json.load(f)
        
        template_config = template_data['template_config']
        
        # Validate template configuration
        errors = self.validate_template_config(template_config)
        if errors:
            raise ValueError(f"Template configuration errors: {', '.join(errors)}")
        
        # Get spatial and feature mappings
        spatial_radius = template_config['spatial_radius']
        feature_set = template_config['feature_set']
        
        radius_mapping = self.schema['spatial_radius_mappings'][spatial_radius]
        feature_mapping = self.schema['feature_set_mappings'][feature_set]
        constants = self.schema['template_constants']
        
        # Generate full configuration
        config = {
            "pipeline_name": template_data.get('output_config', {}).get('pipeline_name', 
                                                                       f"{feature_set.lower()}-{spatial_radius}-{template_config.get('attack_type', 'constoffset')}"),
            "version": "1.0.0",
            "created": datetime.now().strftime("%Y-%m-%d"),
            "template_generated": True,
            "template_parameters": template_config,
            
            "data": {
                "source_file": constants['source_file'],
                "source_type": "csv",
                
                "columns_to_extract": feature_mapping['columns_to_extract'],
                
                "filtering": {
                    "center_longitude": constants['center_longitude'],
                    "center_latitude": constants['center_latitude'],
                    "radius_meters": radius_mapping['radius_meters']
                },
                
                "date_range": constants['date_range'],
                
                "coordinate_conversion": {
                    "enabled": True,
                    "method": "local_projection",
                    "output_columns": ["x_pos", "y_pos"]
                }
            },
            
            "attack": {
                "type": template_config.get('attack_type', 'constant_offset_per_vehicle'),
                "malicious_ratio": template_config.get('malicious_ratio', 0.30),
                "label_column": "isAttacker"
            },
            
            "ml": {
                "features": feature_mapping['ml_features'],
                "label": "isAttacker",
                "train_test_split": constants['train_test_split'],
                "classifiers": constants['classifiers']
            },
            
            "cache": {
                "clean_dataset": f"cache/matrix/{feature_set.lower()}-{spatial_radius}/clean.parquet",
                "attack_dataset": f"cache/matrix/{feature_set.lower()}-{spatial_radius}/attack.parquet"
            },
            
            "output": {
                "results_dir": template_data.get('output_config', {}).get('results_dir', 
                                                                           f"results/matrix/{feature_set.lower()}-{spatial_radius}/"),
                "log_dir": "logs/"
            },
            
            "dask": template_data.get('dask_config', {
                "n_workers": 4,
                "threads_per_worker": 2,
                "memory_limit": "12GB"
            })
        }
        
        # Add attack-specific parameters
        if 'attack_parameters' in template_data:
            config['attack'].update(template_data['attack_parameters'])
        
        return config
    
    def generate_all_matrix_configs(self, output_dir: str = "generated_configs") -> List[str]:
        """Generate configurations for all combinations of spatial radii and feature sets."""
        
        os.makedirs(output_dir, exist_ok=True)
        generated_files = []
        
        spatial_radii = self.schema['template_parameters']['spatial_radius']['values']
        feature_sets = self.schema['template_parameters']['feature_set']['values']
        
        for spatial_radius in spatial_radii:
            for feature_set in feature_sets:
                
                # Create template config for this combination
                template_config = {
                    "template_config": {
                        "spatial_radius": spatial_radius,
                        "feature_set": feature_set,
                        "attack_type": "constant_offset_per_vehicle",
                        "malicious_ratio": 0.30
                    },
                    
                    "attack_parameters": {
                        "offset_distance_min": 100,
                        "offset_distance_max": 200,
                        "offset_direction_min": 0,
                        "offset_direction_max": 360,
                        "seed": 42
                    },
                    
                    "output_config": {
                        "pipeline_name": f"{feature_set.lower()}-{spatial_radius}-constoffset",
                        "results_dir": f"results/matrix/{feature_set.lower()}-{spatial_radius}/",
                        "cache_dir": f"cache/matrix/{feature_set.lower()}-{spatial_radius}/"
                    },
                    
                    "dask_config": {
                        "n_workers": 4 if spatial_radius == "200km" else 3 if spatial_radius == "100km" else 2,
                        "threads_per_worker": 2,
                        "memory_limit": "16GB" if spatial_radius == "200km" else "12GB" if spatial_radius == "100km" else "8GB"
                    }
                }
                
                # Save template config
                template_filename = f"{feature_set.lower()}_{spatial_radius}_template.json"
                template_path = os.path.join(output_dir, template_filename)
                
                with open(template_path, 'w') as f:
                    json.dump(template_config, f, indent=2)
                
                # Generate full pipeline config
                full_config = self.generate_pipeline_config(template_path)
                
                # Save full config
                config_filename = f"{feature_set.lower()}_{spatial_radius}_pipeline_config.json"
                config_path = os.path.join(output_dir, config_filename)
                
                with open(config_path, 'w') as f:
                    json.dump(full_config, f, indent=2)
                
                generated_files.extend([template_path, config_path])
        
        return generated_files


def main():
    """CLI interface for configuration generator."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate Connected Driving pipeline configurations")
    parser.add_argument("--template", "-t", help="Template configuration file")
    parser.add_argument("--output", "-o", help="Output configuration file")
    parser.add_argument("--generate-all", action="store_true", help="Generate all matrix combinations")
    parser.add_argument("--output-dir", default="generated_configs", help="Output directory for generated configs")
    
    args = parser.parse_args()
    
    try:
        generator = ConfigurationGenerator()
        
        if args.generate_all:
            print("Generating all matrix configurations...")
            files = generator.generate_all_matrix_configs(args.output_dir)
            print(f"Generated {len(files)} configuration files in {args.output_dir}/")
            for file in files:
                print(f"  - {file}")
        
        elif args.template:
            config = generator.generate_pipeline_config(args.template)
            
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(config, f, indent=2)
                print(f"Configuration saved to {args.output}")
            else:
                print(json.dumps(config, indent=2))
        
        else:
            print("Error: Either --template or --generate-all must be specified")
            parser.print_help()
            sys.exit(1)
    
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
