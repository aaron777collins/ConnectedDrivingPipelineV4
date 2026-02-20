#!/usr/bin/env python3
"""
Template Validator for Connected Driving Simulation Matrix

This module validates the configurable pipeline template functionality.
It tests configuration parsing, parameter validation, and template generation.
"""

import json
import os
import sys
import tempfile
import traceback
from typing import List, Tuple, Dict, Any

from config_generator import ConfigurationGenerator


class TemplateValidator:
    """Validates template functionality and configurations."""
    
    def __init__(self):
        """Initialize validator."""
        self.test_results = []
        self.generator = None
        
    def run_all_tests(self) -> bool:
        """Run all validation tests."""
        print("🧪 Running Template Validation Tests")
        print("="*50)
        
        tests = [
            ("Schema Loading", self._test_schema_loading),
            ("Configuration Generator", self._test_config_generator),
            ("Sample Configuration Parsing", self._test_sample_configs),
            ("Feature Set Mappings", self._test_feature_set_mappings),
            ("Spatial Radius Mappings", self._test_spatial_mappings),
            ("Parameter Validation", self._test_parameter_validation),
            ("Template Configuration Generation", self._test_template_generation),
            ("All Matrix Combinations", self._test_matrix_combinations),
        ]
        
        for test_name, test_func in tests:
            try:
                print(f"\n🔍 Testing: {test_name}")
                success, message = test_func()
                
                if success:
                    print(f"✅ PASS: {message}")
                else:
                    print(f"❌ FAIL: {message}")
                
                self.test_results.append({
                    'test': test_name,
                    'success': success,
                    'message': message
                })
                
            except Exception as e:
                error_msg = f"Exception in {test_name}: {str(e)}"
                print(f"❌ ERROR: {error_msg}")
                print(f"Traceback: {traceback.format_exc()}")
                
                self.test_results.append({
                    'test': test_name,
                    'success': False,
                    'message': error_msg
                })
        
        # Summary
        passed = sum(1 for r in self.test_results if r['success'])
        total = len(self.test_results)
        
        print(f"\n📊 Test Summary: {passed}/{total} tests passed")
        
        if passed == total:
            print("✅ ALL TESTS PASSED - Template is ready for use!")
            return True
        else:
            print("❌ SOME TESTS FAILED - Template needs fixes")
            for result in self.test_results:
                if not result['success']:
                    print(f"  ❌ {result['test']}: {result['message']}")
            return False
    
    def _test_schema_loading(self) -> Tuple[bool, str]:
        """Test schema file loading."""
        try:
            self.generator = ConfigurationGenerator()
            schema = self.generator.schema
            
            # Check required sections
            required_sections = ['template_parameters', 'feature_set_mappings', 'spatial_radius_mappings', 'template_constants']
            for section in required_sections:
                if section not in schema:
                    return False, f"Missing required schema section: {section}"
            
            return True, f"Schema loaded successfully with {len(schema)} sections"
            
        except Exception as e:
            return False, f"Failed to load schema: {str(e)}"
    
    def _test_config_generator(self) -> Tuple[bool, str]:
        """Test configuration generator initialization."""
        try:
            if not self.generator:
                self.generator = ConfigurationGenerator()
            
            # Test generator methods exist
            methods = ['validate_template_config', 'generate_pipeline_config', 'generate_all_matrix_configs']
            for method in methods:
                if not hasattr(self.generator, method):
                    return False, f"Missing required method: {method}"
            
            return True, "Configuration generator initialized with all required methods"
            
        except Exception as e:
            return False, f"Failed to initialize generator: {str(e)}"
    
    def _test_sample_configs(self) -> Tuple[bool, str]:
        """Test sample configuration files."""
        sample_dir = "sample_configs"
        
        if not os.path.exists(sample_dir):
            return False, "Sample configs directory not found"
        
        sample_files = [f for f in os.listdir(sample_dir) if f.endswith('.json')]
        
        if len(sample_files) == 0:
            return False, "No sample configuration files found"
        
        parsed_count = 0
        
        for sample_file in sample_files:
            try:
                file_path = os.path.join(sample_dir, sample_file)
                with open(file_path, 'r') as f:
                    config = json.load(f)
                
                # Check required sections
                if 'template_config' not in config:
                    return False, f"{sample_file}: Missing template_config section"
                
                template_config = config['template_config']
                
                # Validate template config
                errors = self.generator.validate_template_config(template_config)
                if errors:
                    return False, f"{sample_file}: Validation errors: {', '.join(errors)}"
                
                parsed_count += 1
                
            except Exception as e:
                return False, f"Failed to parse {sample_file}: {str(e)}"
        
        return True, f"Successfully parsed and validated {parsed_count} sample configurations"
    
    def _test_feature_set_mappings(self) -> Tuple[bool, str]:
        """Test feature set mappings."""
        expected_feature_sets = ['BASIC', 'BASIC_WITH_ID', 'MOVEMENT', 'MOVEMENT_WITH_ID', 'EXTENDED', 'EXTENDED_WITH_ID']
        feature_mappings = self.generator.schema['feature_set_mappings']
        
        for feature_set in expected_feature_sets:
            if feature_set not in feature_mappings:
                return False, f"Missing feature set mapping: {feature_set}"
            
            mapping = feature_mappings[feature_set]
            
            # Check required fields
            required_fields = ['columns_to_extract', 'ml_features', 'includes_id']
            for field in required_fields:
                if field not in mapping:
                    return False, f"{feature_set}: Missing required field: {field}"
            
            # Validate ID consistency
            includes_id = mapping['includes_id']
            has_id_in_columns = 'coredata_id' in mapping['columns_to_extract']
            has_id_in_features = 'coredata_id' in mapping['ml_features']
            
            if includes_id and not (has_id_in_columns and has_id_in_features):
                return False, f"{feature_set}: includes_id=True but coredata_id not in columns or features"
            
            if not includes_id and (has_id_in_columns or has_id_in_features):
                return False, f"{feature_set}: includes_id=False but coredata_id found in columns or features"
            
            # Check basic columns are present
            basic_columns = ['x_pos', 'y_pos', 'coredata_elevation']
            for col in basic_columns:
                if col not in mapping['ml_features']:
                    return False, f"{feature_set}: Missing basic column: {col}"
        
        return True, f"All {len(expected_feature_sets)} feature set mappings are valid"
    
    def _test_spatial_mappings(self) -> Tuple[bool, str]:
        """Test spatial radius mappings."""
        expected_radii = ['200km', '100km', '2km']
        expected_meters = [200000, 100000, 2000]
        
        spatial_mappings = self.generator.schema['spatial_radius_mappings']
        
        for radius, expected_m in zip(expected_radii, expected_meters):
            if radius not in spatial_mappings:
                return False, f"Missing spatial radius mapping: {radius}"
            
            mapping = spatial_mappings[radius]
            
            if 'radius_meters' not in mapping:
                return False, f"{radius}: Missing radius_meters field"
            
            if mapping['radius_meters'] != expected_m:
                return False, f"{radius}: Expected {expected_m}m, got {mapping['radius_meters']}m"
        
        return True, f"All {len(expected_radii)} spatial radius mappings are correct"
    
    def _test_parameter_validation(self) -> Tuple[bool, str]:
        """Test parameter validation logic."""
        
        # Test valid config
        valid_config = {
            'spatial_radius': '200km',
            'feature_set': 'BASIC',
            'malicious_ratio': 0.30
        }
        
        errors = self.generator.validate_template_config(valid_config)
        if errors:
            return False, f"Valid config rejected: {', '.join(errors)}"
        
        # Test invalid spatial radius
        invalid_config1 = {
            'spatial_radius': '500km',  # Invalid
            'feature_set': 'BASIC'
        }
        
        errors = self.generator.validate_template_config(invalid_config1)
        if not errors:
            return False, "Invalid spatial_radius not caught"
        
        # Test invalid feature set
        invalid_config2 = {
            'spatial_radius': '200km',
            'feature_set': 'INVALID_SET'  # Invalid
        }
        
        errors = self.generator.validate_template_config(invalid_config2)
        if not errors:
            return False, "Invalid feature_set not caught"
        
        # Test invalid malicious ratio
        invalid_config3 = {
            'spatial_radius': '200km',
            'feature_set': 'BASIC',
            'malicious_ratio': 0.8  # Invalid (too high)
        }
        
        errors = self.generator.validate_template_config(invalid_config3)
        if not errors:
            return False, "Invalid malicious_ratio not caught"
        
        # Test missing required parameters
        invalid_config4 = {
            'feature_set': 'BASIC'
            # Missing spatial_radius
        }
        
        errors = self.generator.validate_template_config(invalid_config4)
        if not errors:
            return False, "Missing required parameter not caught"
        
        return True, "Parameter validation working correctly"
    
    def _test_template_generation(self) -> Tuple[bool, str]:
        """Test template configuration generation."""
        
        # Create temporary template config
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            test_config = {
                "template_config": {
                    "spatial_radius": "100km",
                    "feature_set": "MOVEMENT_WITH_ID",
                    "attack_type": "constant_offset_per_vehicle",
                    "malicious_ratio": 0.25
                },
                "attack_parameters": {
                    "offset_distance_min": 150,
                    "offset_distance_max": 300,
                    "seed": 123
                },
                "output_config": {
                    "pipeline_name": "test-pipeline",
                    "results_dir": "results/test/"
                }
            }
            
            json.dump(test_config, f, indent=2)
            temp_file = f.name
        
        try:
            # Generate pipeline config
            pipeline_config = self.generator.generate_pipeline_config(temp_file)
            
            # Validate generated config
            required_sections = ['data', 'attack', 'ml', 'cache', 'output', 'dask']
            for section in required_sections:
                if section not in pipeline_config:
                    return False, f"Generated config missing section: {section}"
            
            # Check template parameters are preserved
            if 'template_parameters' not in pipeline_config:
                return False, "Generated config missing template_parameters"
            
            template_params = pipeline_config['template_parameters']
            if template_params['spatial_radius'] != '100km':
                return False, "Template parameters not preserved correctly"
            
            # Check feature mapping applied
            expected_features = self.generator.schema['feature_set_mappings']['MOVEMENT_WITH_ID']['ml_features']
            actual_features = pipeline_config['ml']['features']
            
            if set(actual_features) != set(expected_features):
                return False, f"Feature mapping incorrect. Expected: {expected_features}, Got: {actual_features}"
            
            # Check spatial mapping applied
            expected_radius = 100000  # 100km in meters
            actual_radius = pipeline_config['data']['filtering']['radius_meters']
            
            if actual_radius != expected_radius:
                return False, f"Spatial mapping incorrect. Expected: {expected_radius}m, Got: {actual_radius}m"
            
            return True, "Template configuration generation working correctly"
        
        finally:
            # Clean up temporary file
            os.unlink(temp_file)
    
    def _test_matrix_combinations(self) -> Tuple[bool, str]:
        """Test all matrix combination generation."""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                # Generate all matrix configurations
                generated_files = self.generator.generate_all_matrix_configs(temp_dir)
                
                # Expected combinations: 3 radii × 6 feature sets = 18 combinations
                # Each combination generates 2 files (template + pipeline config) = 36 files
                expected_file_count = 3 * 6 * 2  # 36 files
                
                if len(generated_files) != expected_file_count:
                    return False, f"Expected {expected_file_count} files, got {len(generated_files)}"
                
                # Check that all combinations are present
                radii = ['200km', '100km', '2km']
                feature_sets = ['basic', 'basic_with_id', 'movement', 'movement_with_id', 'extended', 'extended_with_id']
                
                expected_prefixes = []
                for radius in radii:
                    for feature_set in feature_sets:
                        expected_prefixes.append(f"{feature_set}_{radius}")
                
                generated_prefixes = set()
                for file_path in generated_files:
                    filename = os.path.basename(file_path)
                    for prefix in expected_prefixes:
                        if filename.startswith(prefix):
                            generated_prefixes.add(prefix)
                            break
                
                if len(generated_prefixes) != len(expected_prefixes):
                    missing = set(expected_prefixes) - generated_prefixes
                    return False, f"Missing combinations: {missing}"
                
                # Validate a few generated configs
                config_files = [f for f in generated_files if 'pipeline_config' in f]
                
                for i, config_file in enumerate(config_files[:3]):  # Test first 3
                    with open(config_file, 'r') as f:
                        config = json.load(f)
                    
                    # Basic validation
                    required_sections = ['data', 'attack', 'ml', 'template_parameters']
                    for section in required_sections:
                        if section not in config:
                            return False, f"Generated config {config_file} missing section: {section}"
                
                return True, f"Successfully generated and validated all {len(expected_prefixes)} matrix combinations ({len(generated_files)} files)"
            
            except Exception as e:
                return False, f"Matrix generation failed: {str(e)}"


def main():
    """CLI interface for template validator."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate Connected Driving pipeline templates")
    parser.add_argument("--quick", action="store_true", help="Run quick validation tests only")
    
    args = parser.parse_args()
    
    validator = TemplateValidator()
    
    if args.quick:
        # Run basic validation only
        print("🚀 Running Quick Validation Tests")
        success = True
        
        try:
            validator.generator = ConfigurationGenerator()
            print("✅ Configuration generator loaded successfully")
        except Exception as e:
            print(f"❌ Failed to load configuration generator: {e}")
            success = False
        
        if success:
            print("✅ Quick validation passed - template appears functional")
        else:
            print("❌ Quick validation failed")
            sys.exit(1)
    
    else:
        # Run full validation
        success = validator.run_all_tests()
        
        if not success:
            sys.exit(1)


if __name__ == "__main__":
    main()
