"""
Example: Using the UDF Registry System

This script demonstrates how to use the UDF Registry for centralized UDF management.

Author: PySpark Migration Team
Created: 2026-01-17
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from Helpers.SparkUDFs import (
    get_registry,
    initialize_udf_registry,
    UDFCategory
)


def example_1_basic_usage():
    """Example 1: Basic registry usage"""
    print("\n" + "="*80)
    print("EXAMPLE 1: Basic Registry Usage")
    print("="*80)

    # Initialize the registry (call once during app startup)
    initialize_udf_registry()

    # Get the registry instance
    registry = get_registry()

    # List all available UDFs
    print(f"\nTotal registered UDFs: {registry.count()}")
    print(f"Available UDFs: {', '.join(registry.list_all())}")


def example_2_list_by_category():
    """Example 2: List UDFs by category"""
    print("\n" + "="*80)
    print("EXAMPLE 2: List UDFs by Category")
    print("="*80)

    registry = get_registry()

    # Get all categories
    categories = registry.get_categories()
    print(f"\nAvailable categories: {[c.value for c in categories]}")

    # List UDFs in each category
    for category in categories:
        udfs = registry.list_by_category(category)
        print(f"\n{category.value.upper()} ({len(udfs)} UDFs):")
        for udf_name in udfs:
            metadata = registry.get_metadata(udf_name)
            print(f"  - {udf_name}: {metadata.description}")


def example_3_udf_metadata():
    """Example 3: Get UDF metadata"""
    print("\n" + "="*80)
    print("EXAMPLE 3: Get UDF Metadata")
    print("="*80)

    registry = get_registry()

    # Get detailed information about a specific UDF
    udf_name = 'hex_to_decimal'
    metadata = registry.get_metadata(udf_name)

    print(f"\nUDF: {metadata.name} (v{metadata.version})")
    print(f"Description: {metadata.description}")
    print(f"Category: {metadata.category.value}")
    print(f"Input Types: {', '.join(metadata.input_types)}")
    print(f"Output Type: {metadata.output_type}")
    print(f"\nUsage Example:")
    print(metadata.example)


def example_4_use_in_dataframe():
    """Example 4: Use registry UDF in DataFrame operations"""
    print("\n" + "="*80)
    print("EXAMPLE 4: Use Registry UDF in DataFrame Operations")
    print("="*80)

    # Create a local Spark session
    spark = SparkSession.builder \
        .appName("UDFRegistryExample") \
        .master("local[2]") \
        .getOrCreate()

    try:
        registry = get_registry()

        # Create sample data
        data = [
            ('0x1a', 'POINT (-105.93 41.25)'),
            ('0x2b', 'POINT (-105.94 41.26)'),
            ('0x3c', 'POINT (-105.95 41.27)'),
        ]
        df = spark.createDataFrame(data, ['coreData_id', 'coreData_position'])

        print("\nOriginal DataFrame:")
        df.show(truncate=False)

        # Retrieve UDFs from registry
        hex_udf = registry.get('hex_to_decimal')
        point_x_udf = registry.get('point_to_x')
        point_y_udf = registry.get('point_to_y')

        # Apply UDFs
        df_result = df \
            .withColumn('id_decimal', hex_udf(col('coreData_id'))) \
            .withColumn('longitude', point_x_udf(col('coreData_position'))) \
            .withColumn('latitude', point_y_udf(col('coreData_position')))

        print("\nTransformed DataFrame (using registry UDFs):")
        df_result.show(truncate=False)

        # Calculate distance from reference point
        geodesic_udf = registry.get('geodesic_distance')
        df_with_distance = df_result.withColumn(
            'distance_from_cheyenne',
            geodesic_udf(
                col('latitude'),
                col('longitude'),
                lit(41.1400),  # Cheyenne, WY latitude
                lit(-104.8202)  # Cheyenne, WY longitude
            )
        )

        print("\nWith Distance Calculation:")
        df_with_distance.select('id_decimal', 'latitude', 'longitude', 'distance_from_cheyenne').show()

    finally:
        spark.stop()


def example_5_generate_documentation():
    """Example 5: Generate UDF documentation"""
    print("\n" + "="*80)
    print("EXAMPLE 5: Generate UDF Documentation")
    print("="*80)

    registry = get_registry()

    # Generate comprehensive documentation
    doc = registry.generate_documentation()

    print("\nGenerated Documentation (first 1000 characters):")
    print(doc[:1000])
    print("\n... (documentation continues)")

    # Optionally save to file
    # with open('UDF_DOCUMENTATION.md', 'w') as f:
    #     f.write(doc)
    # print("\nFull documentation saved to UDF_DOCUMENTATION.md")


def example_6_check_udf_exists():
    """Example 6: Check if UDF exists before using"""
    print("\n" + "="*80)
    print("EXAMPLE 6: Check UDF Existence")
    print("="*80)

    registry = get_registry()

    udfs_to_check = ['hex_to_decimal', 'nonexistent_udf', 'geodesic_distance']

    print("\nChecking UDF existence:")
    for udf_name in udfs_to_check:
        exists = registry.exists(udf_name)
        status = "✓ Registered" if exists else "✗ Not found"
        print(f"  {udf_name}: {status}")


def main():
    """Run all examples"""
    print("\n" + "="*80)
    print("UDF Registry System - Usage Examples")
    print("="*80)

    # Initialize registry once
    initialize_udf_registry()

    # Run examples
    example_1_basic_usage()
    example_2_list_by_category()
    example_3_udf_metadata()
    example_4_use_in_dataframe()
    example_5_generate_documentation()
    example_6_check_udf_exists()

    print("\n" + "="*80)
    print("All examples completed successfully!")
    print("="*80 + "\n")


if __name__ == '__main__':
    main()
