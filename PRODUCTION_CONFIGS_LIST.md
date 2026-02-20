# Production Pipeline Configurations

**Generated:** 2026-02-20
**Total Configurations:** 18
**Attack Type:** Random Offset 100-200m
**Configuration Matrix:** 3 spatial radii × 6 feature sets

## Configuration Matrix

### Spatial Radii
- **200km** - Large area analysis (200,000m radius)
- **100km** - Medium area analysis (100,000m radius) 
- **2km** - Local area analysis (2,000m radius)

### Feature Sets
- **BASIC** - Position and elevation only
- **BASIC_WITH_ID** - Basic + vehicle ID
- **MOVEMENT** - Basic + heading and speed
- **MOVEMENT_WITH_ID** - Movement + vehicle ID
- **EXTENDED** - Movement + acceleration data
- **EXTENDED_WITH_ID** - Extended + vehicle ID

## Generated Configuration Files

### 200km Radius Configurations
1. `basic_200km_pipeline_config.json` - BASIC features, 200km radius
2. `basic_with_id_200km_pipeline_config.json` - BASIC_WITH_ID features, 200km radius
3. `movement_200km_pipeline_config.json` - MOVEMENT features, 200km radius
4. `movement_with_id_200km_pipeline_config.json` - MOVEMENT_WITH_ID features, 200km radius
5. `extended_200km_pipeline_config.json` - EXTENDED features, 200km radius
6. `extended_with_id_200km_pipeline_config.json` - EXTENDED_WITH_ID features, 200km radius

### 100km Radius Configurations  
7. `basic_100km_pipeline_config.json` - BASIC features, 100km radius
8. `basic_with_id_100km_pipeline_config.json` - BASIC_WITH_ID features, 100km radius
9. `movement_100km_pipeline_config.json` - MOVEMENT features, 100km radius
10. `movement_with_id_100km_pipeline_config.json` - MOVEMENT_WITH_ID features, 100km radius
11. `extended_100km_pipeline_config.json` - EXTENDED features, 100km radius
12. `extended_with_id_100km_pipeline_config.json` - EXTENDED_WITH_ID features, 100km radius

### 2km Radius Configurations
13. `basic_2km_pipeline_config.json` - BASIC features, 2km radius
14. `basic_with_id_2km_pipeline_config.json` - BASIC_WITH_ID features, 2km radius
15. `movement_2km_pipeline_config.json` - MOVEMENT features, 2km radius
16. `movement_with_id_2km_pipeline_config.json` - MOVEMENT_WITH_ID features, 2km radius
17. `extended_2km_pipeline_config.json` - EXTENDED features, 2km radius
18. `extended_with_id_2km_pipeline_config.json` - EXTENDED_WITH_ID features, 2km radius

## Attack Parameters

All configurations use **Random Offset 100-200m** attack with:
- **Attack Type:** `random_offset`
- **Offset Distance:** 100-200 meters
- **Offset Direction:** 0-360 degrees (random)
- **Malicious Ratio:** 30% of vehicles
- **Random Seed:** 42 (for reproducibility)

## Validation Status

✅ All 18 configurations generated successfully  
✅ All configurations pass template validation  
✅ All configurations use correct attack type (random_offset)  
✅ Sample configurations tested and verified functional  
✅ Configuration matrix complete (3 radii × 6 feature sets = 18 total)

## File Locations

- **Production Configs:** `/home/ubuntu/repos/ConnectedDrivingPipelineV4/production_configs/`
- **Template Generator:** `/home/ubuntu/repos/ConnectedDrivingPipelineV4/templates/config_generator.py`
- **Validator:** `/home/ubuntu/repos/ConnectedDrivingPipelineV4/templates/template_validator.py`
- **Schema:** `/home/ubuntu/repos/ConnectedDrivingPipelineV4/templates/config_schema.json`
