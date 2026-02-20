# Connected Driving Simulation Matrix - Configurable Pipeline Template

This directory contains the configurable pipeline template system that supports all spatial radii (200km, 100km, 2km) and feature sets (BASIC, BASIC_WITH_ID, MOVEMENT, MOVEMENT_WITH_ID, EXTENDED, EXTENDED_WITH_ID).

## Validation Status
✅ ALL TESTS PASSED - Template system is ready for use!
- 8/8 validation tests passed
- All 18 matrix combinations (3 radii × 6 feature sets) successfully generated
- Template configuration generation working correctly
- Feature set and spatial radius mappings validated

## Files Created
- config_schema.json - Template parameter definitions and mappings  
- config_generator.py - Generates pipeline configs from templates
- configurable_pipeline_template.py - Main adaptable pipeline template
- template_validator.py - Comprehensive validation testing
- sample_configs/ - 3 sample template configurations
- test_matrix_configs/ - Generated 18 combinations (36 files)

## Usage
1. Generate single config: python3 config_generator.py --template sample_configs/basic_200km_sample.json
2. Generate all combinations: python3 config_generator.py --generate-all  
3. Validate system: python3 template_validator.py
4. Run pipeline: python3 configurable_pipeline_template.py --config <config_file>

Ready for cdp-1-3 to generate production configurations!
