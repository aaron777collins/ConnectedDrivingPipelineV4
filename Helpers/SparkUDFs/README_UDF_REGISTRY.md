# UDF Registry System Documentation

## Overview

The UDF Registry provides centralized management of all PySpark UDFs (User-Defined Functions) used in the Connected Driving Pipeline. It enables easy discovery, documentation, and retrieval of UDFs without needing to remember specific module paths.

## Benefits

1. **Centralized Management**: All UDFs are registered in one place
2. **Easy Discovery**: List UDFs by category (geospatial, conversion, etc.)
3. **Metadata Tracking**: Each UDF has description, input/output types, and examples
4. **Simplified Imports**: Retrieve UDFs by name instead of importing from specific modules
5. **Documentation Generation**: Automatically generate documentation for all UDFs

## Quick Start

### Basic Usage

```python
from Helpers.SparkUDFs import get_registry, initialize_udf_registry
from pyspark.sql.functions import col

# Initialize the registry (call once during app startup)
initialize_udf_registry()

# Get the registry instance
registry = get_registry()

# Retrieve a UDF by name
hex_udf = registry.get('hex_to_decimal')

# Use the UDF in DataFrame operations
df = df.withColumn('id_decimal', hex_udf(col('coreData_id')))
```

### List Available UDFs

```python
from Helpers.SparkUDFs import get_registry
from Helpers.SparkUDFs.UDFRegistry import UDFCategory

registry = get_registry()

# List all registered UDFs
all_udfs = registry.list_all()
print(f"Total UDFs: {len(all_udfs)}")
print(f"UDFs: {', '.join(all_udfs)}")

# List UDFs by category
geospatial_udfs = registry.list_by_category(UDFCategory.GEOSPATIAL)
conversion_udfs = registry.list_by_category(UDFCategory.CONVERSION)

print(f"\nGeospatial UDFs: {', '.join(geospatial_udfs)}")
print(f"Conversion UDFs: {', '.join(conversion_udfs)}")
```

### Get UDF Metadata

```python
from Helpers.SparkUDFs import get_registry

registry = get_registry()

# Get detailed information about a UDF
metadata = registry.get_metadata('hex_to_decimal')

print(f"Name: {metadata.name}")
print(f"Description: {metadata.description}")
print(f"Category: {metadata.category.value}")
print(f"Input Types: {metadata.input_types}")
print(f"Output Type: {metadata.output_type}")
print(f"\nExample:\n{metadata.example}")
```

### Generate Documentation

```python
from Helpers.SparkUDFs import get_registry

registry = get_registry()

# Generate comprehensive documentation for all UDFs
doc = registry.generate_documentation()
print(doc)

# Or save to a file
with open('UDF_DOCUMENTATION.md', 'w') as f:
    f.write(doc)
```

## Architecture

### Components

1. **UDFRegistry**: Singleton class that stores and manages all UDFs
2. **UDFMetadata**: Dataclass containing metadata for each UDF
3. **UDFCategory**: Enum defining UDF categories (GEOSPATIAL, CONVERSION, etc.)
4. **RegisterUDFs**: Module that auto-registers all existing UDFs
5. **initialize_udf_registry()**: Function to populate the registry

### Registry Flow

```
Application Startup
    ↓
initialize_udf_registry()
    ↓
Registers all UDFs with metadata
    ↓
Registry ready for use
    ↓
get_registry().get('udf_name')
    ↓
UDF retrieved and used in DataFrame operations
```

## Registered UDFs

### Geospatial UDFs

| Name | Description | Input Types | Output Type |
|------|-------------|-------------|-------------|
| `point_to_tuple` | Convert WKT POINT to (x, y) struct | string | struct<x:double,y:double> |
| `point_to_x` | Extract X (longitude) from WKT POINT | string | double |
| `point_to_y` | Extract Y (latitude) from WKT POINT | string | double |
| `geodesic_distance` | Calculate distance between lat/long points (WGS84) | double, double, double, double | double |
| `xy_distance` | Calculate Euclidean distance between XY points | double, double, double, double | double |

### Conversion UDFs

| Name | Description | Input Types | Output Type |
|------|-------------|-------------|-------------|
| `hex_to_decimal` | Convert hex string to decimal (handles trailing .0) | string | long |
| `direction_and_dist_to_xy` | Calculate new XY coords from direction/distance | double, double, int, int | struct<new_x:double,new_y:double> |

## Usage Patterns

### Pattern 1: Direct Import (Legacy)

```python
from Helpers.SparkUDFs import hex_to_decimal_udf, point_to_x_udf

df = df.withColumn('id_decimal', hex_to_decimal_udf(col('coreData_id')))
df = df.withColumn('x_pos', point_to_x_udf(col('coreData_position')))
```

**Pros:**
- Familiar import pattern
- IDE autocomplete support
- Slightly less verbose

**Cons:**
- Need to know exact UDF names and modules
- No centralized documentation
- Hard to discover available UDFs

### Pattern 2: Registry-Based (Recommended)

```python
from Helpers.SparkUDFs import get_registry, initialize_udf_registry

initialize_udf_registry()  # Call once at startup
registry = get_registry()

hex_udf = registry.get('hex_to_decimal')
point_x_udf = registry.get('point_to_x')

df = df.withColumn('id_decimal', hex_udf(col('coreData_id')))
df = df.withColumn('x_pos', point_x_udf(col('coreData_position')))
```

**Pros:**
- Centralized UDF management
- Easy discovery and documentation
- Metadata tracking
- Simplified UDF lookup

**Cons:**
- Requires initialization step
- String-based lookup (no IDE autocomplete)
- Slightly more verbose

### Pattern 3: Hybrid Approach

```python
from Helpers.SparkUDFs import (
    # Direct imports for commonly used UDFs
    hex_to_decimal_udf,
    point_to_x_udf,
    # Registry for discovery and documentation
    get_registry,
    initialize_udf_registry
)

# Use direct imports for common operations
df = df.withColumn('id_decimal', hex_to_decimal_udf(col('coreData_id')))

# Use registry for dynamic UDF selection
registry = get_registry()
if needs_geodesic:
    dist_udf = registry.get('geodesic_distance')
else:
    dist_udf = registry.get('xy_distance')

df = df.withColumn('distance', dist_udf(col('x1'), col('y1'), lit(x2), lit(y2)))
```

## Adding New UDFs to the Registry

When creating new UDFs, follow these steps to register them:

### Step 1: Create the UDF

```python
# In Helpers/SparkUDFs/MyNewUDFs.py
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def my_new_udf(input_val):
    """My new UDF that does something"""
    # UDF logic here
    return str(input_val).upper()
```

### Step 2: Register in RegisterUDFs.py

```python
# In Helpers/SparkUDFs/RegisterUDFs.py
from Helpers.SparkUDFs.MyNewUDFs import my_new_udf

def initialize_udf_registry():
    registry = UDFRegistry.get_instance()

    # ... existing registrations ...

    # Register your new UDF
    registry.register(
        name='my_new_udf',
        udf_func=my_new_udf,
        description='Converts input to uppercase string',
        category=UDFCategory.UTILITY,  # or create a new category
        input_types=['any'],
        output_type='string',
        example=(
            "from pyspark.sql.functions import col\n"
            "df = df.withColumn('upper_val', my_new_udf(col('value')))"
        ),
        version='1.0.0'
    )

    registry.mark_initialized()
    return registry
```

### Step 3: Export from __init__.py

```python
# In Helpers/SparkUDFs/__init__.py
from Helpers.SparkUDFs.MyNewUDFs import my_new_udf

__all__ = [
    # ... existing exports ...
    'my_new_udf',
]
```

## Testing

The UDF Registry has comprehensive test coverage:

```bash
# Run registry tests
python3 -m pytest Test/test_udf_registry.py -v

# Run with coverage
python3 -m pytest Test/test_udf_registry.py --cov=Helpers/SparkUDFs/UDFRegistry --cov-report=html
```

## API Reference

### UDFRegistry Methods

#### `get_instance() -> UDFRegistry`
Get the singleton registry instance.

#### `register(name, udf_func, description, category, input_types, output_type, example, version)`
Register a UDF with metadata.

#### `get(name: str) -> Callable`
Retrieve a UDF function by name.

#### `get_metadata(name: str) -> UDFMetadata`
Get metadata for a UDF.

#### `list_all() -> List[str]`
List all registered UDF names.

#### `list_by_category(category: UDFCategory) -> List[str]`
List UDFs in a specific category.

#### `exists(name: str) -> bool`
Check if a UDF is registered.

#### `count() -> int`
Get total number of registered UDFs.

#### `generate_documentation() -> str`
Generate markdown documentation for all UDFs.

### Helper Functions

#### `get_registry() -> UDFRegistry`
Convenience function to get registry instance.

#### `initialize_udf_registry() -> UDFRegistry`
Initialize registry with all UDFs. Call once at startup.

## Best Practices

1. **Initialize Once**: Call `initialize_udf_registry()` once during application startup
2. **Use Registry for Discovery**: Use the registry to explore available UDFs and their usage
3. **Direct Imports for Production**: Use direct imports in production code for better IDE support
4. **Document New UDFs**: Always provide complete metadata when registering new UDFs
5. **Category Organization**: Use appropriate categories to keep UDFs organized
6. **Version Tracking**: Update version numbers when UDF behavior changes

## Future Enhancements

Potential improvements for the UDF Registry:

- [ ] Performance metrics tracking (execution time, invocation count)
- [ ] UDF validation (test inputs/outputs during registration)
- [ ] Deprecation warnings for old UDFs
- [ ] Auto-generation of UDF unit tests
- [ ] Integration with Spark UI for UDF performance monitoring
- [ ] UDF aliasing (multiple names for same UDF)
- [ ] Dynamic UDF loading from configuration files

## See Also

- `Test/test_udf_registry.py` - Comprehensive test suite
- `Helpers/SparkUDFs/GeospatialUDFs.py` - Geospatial UDF implementations
- `Helpers/SparkUDFs/ConversionUDFs.py` - Conversion UDF implementations
- Migration Plan Section 3.7 - UDF Registry Design
