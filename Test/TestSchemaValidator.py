"""
Unit tests for SchemaValidator utilities
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    LongType, TimestampType, ArrayType
)

from Helpers.SchemaValidator import (
    SchemaValidator,
    SchemaValidationError,
    validate_bsm_raw_schema,
    validate_bsm_processed_schema
)
from Schemas import get_bsm_raw_schema, get_bsm_processed_schema


class TestSchemaValidator(unittest.TestCase):
    """Test cases for SchemaValidator class"""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests"""
        cls.spark = SparkSession.builder \
            .appName("SchemaValidatorTest") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Tear down Spark session"""
        cls.spark.stop()

    def test_validate_schema_exact_match(self):
        """Test that validation passes for exact schema match"""
        schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("value", DoubleType(), nullable=True)
        ])

        df = self.spark.createDataFrame(
            [(1, "Alice", 100.5), (2, "Bob", 200.7)],
            schema
        )

        is_valid, errors = SchemaValidator.validate_schema(df, schema, raise_on_error=False)

        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)

    def test_validate_schema_missing_column(self):
        """Test that validation fails when column is missing"""
        expected_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("value", DoubleType(), nullable=True)
        ])

        actual_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True)
        ])

        df = self.spark.createDataFrame([(1, "Alice"), (2, "Bob")], actual_schema)

        is_valid, errors = SchemaValidator.validate_schema(
            df, expected_schema, raise_on_error=False
        )

        self.assertFalse(is_valid)
        self.assertTrue(any("Missing columns" in e for e in errors))
        self.assertTrue(any("value" in e for e in errors))

    def test_validate_schema_extra_column(self):
        """Test that validation fails when extra column exists"""
        expected_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True)
        ])

        actual_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("extra", DoubleType(), nullable=True)
        ])

        df = self.spark.createDataFrame(
            [(1, "Alice", 100.5), (2, "Bob", 200.7)],
            actual_schema
        )

        is_valid, errors = SchemaValidator.validate_schema(
            df, expected_schema, raise_on_error=False
        )

        self.assertFalse(is_valid)
        self.assertTrue(any("Extra columns" in e for e in errors))
        self.assertTrue(any("extra" in e for e in errors))

    def test_validate_schema_type_mismatch(self):
        """Test that validation fails when column type is wrong"""
        expected_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("value", DoubleType(), nullable=True)
        ])

        actual_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("value", StringType(), nullable=True)  # Wrong type
        ])

        df = self.spark.createDataFrame([(1, "100.5"), (2, "200.7")], actual_schema)

        is_valid, errors = SchemaValidator.validate_schema(
            df, expected_schema, raise_on_error=False
        )

        self.assertFalse(is_valid)
        self.assertTrue(any("type mismatch" in e for e in errors))

    def test_validate_schema_wrong_order(self):
        """Test that validation fails with strict_order when columns are out of order"""
        expected_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("value", DoubleType(), nullable=True)
        ])

        actual_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("value", DoubleType(), nullable=True),  # Out of order
            StructField("name", StringType(), nullable=True)
        ])

        df = self.spark.createDataFrame([(1, 100.5, "Alice")], actual_schema)

        # Without strict_order, should pass
        is_valid, errors = SchemaValidator.validate_schema(
            df, expected_schema, strict_order=False, raise_on_error=False
        )
        self.assertTrue(is_valid)

        # With strict_order, should fail
        is_valid, errors = SchemaValidator.validate_schema(
            df, expected_schema, strict_order=True, raise_on_error=False
        )
        self.assertFalse(is_valid)
        self.assertTrue(any("order mismatch" in e for e in errors))

    def test_validate_schema_nullability_mismatch(self):
        """Test that validation fails with strict_nullability when nullable differs"""
        expected_schema = StructType([
            StructField("id", IntegerType(), nullable=False)
        ])

        actual_schema = StructType([
            StructField("id", IntegerType(), nullable=True)  # Different nullability
        ])

        df = self.spark.createDataFrame([(1,), (2,)], actual_schema)

        # Without strict_nullability, should pass
        is_valid, errors = SchemaValidator.validate_schema(
            df, expected_schema, strict_nullability=False, raise_on_error=False
        )
        self.assertTrue(is_valid)

        # With strict_nullability, should fail
        is_valid, errors = SchemaValidator.validate_schema(
            df, expected_schema, strict_nullability=True, raise_on_error=False
        )
        self.assertFalse(is_valid)
        self.assertTrue(any("nullability mismatch" in e for e in errors))

    def test_validate_schema_raises_exception(self):
        """Test that validation raises exception when raise_on_error is True"""
        expected_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True)
        ])

        actual_schema = StructType([
            StructField("id", IntegerType(), nullable=False)
        ])

        df = self.spark.createDataFrame([(1,), (2,)], actual_schema)

        with self.assertRaises(SchemaValidationError):
            SchemaValidator.validate_schema(df, expected_schema, raise_on_error=True)

    def test_get_schema_diff(self):
        """Test that get_schema_diff returns correct diff information"""
        expected_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("value", DoubleType(), nullable=True)
        ])

        actual_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", IntegerType(), nullable=False),  # Wrong type and nullability
            StructField("extra", StringType(), nullable=True)  # Extra column
        ])

        df = self.spark.createDataFrame([(1, 123, "extra")], actual_schema)
        diff = SchemaValidator.get_schema_diff(df, expected_schema)

        self.assertEqual(diff['missing_columns'], ['value'])
        self.assertEqual(diff['extra_columns'], ['extra'])
        self.assertIn('name', diff['type_mismatches'])
        self.assertIn('name', diff['nullability_mismatches'])

    def test_validate_columns_exist(self):
        """Test that validate_columns_exist correctly checks for column presence"""
        schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True)
        ])

        df = self.spark.createDataFrame([(1, "Alice"), (2, "Bob")], schema)

        # All columns exist
        all_exist, missing = SchemaValidator.validate_columns_exist(
            df, ["id", "name"], raise_on_error=False
        )
        self.assertTrue(all_exist)
        self.assertEqual(missing, [])

        # Some columns missing
        all_exist, missing = SchemaValidator.validate_columns_exist(
            df, ["id", "name", "value"], raise_on_error=False
        )
        self.assertFalse(all_exist)
        self.assertEqual(missing, ["value"])

    def test_validate_columns_exist_raises_exception(self):
        """Test that validate_columns_exist raises exception when columns missing"""
        schema = StructType([
            StructField("id", IntegerType(), nullable=False)
        ])

        df = self.spark.createDataFrame([(1,), (2,)], schema)

        with self.assertRaises(SchemaValidationError):
            SchemaValidator.validate_columns_exist(df, ["id", "missing"], raise_on_error=True)

    def test_validate_column_types(self):
        """Test that validate_column_types correctly checks column data types"""
        schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("value", DoubleType(), nullable=True)
        ])

        df = self.spark.createDataFrame([(1, "Alice", 100.5)], schema)

        # All types match
        all_match, mismatches = SchemaValidator.validate_column_types(
            df,
            {
                "id": IntegerType(),
                "name": StringType(),
                "value": DoubleType()
            },
            raise_on_error=False
        )
        self.assertTrue(all_match)
        self.assertEqual(mismatches, {})

        # Some types don't match
        all_match, mismatches = SchemaValidator.validate_column_types(
            df,
            {
                "id": StringType(),  # Wrong type
                "name": StringType(),
                "value": IntegerType()  # Wrong type
            },
            raise_on_error=False
        )
        self.assertFalse(all_match)
        self.assertIn("id", mismatches)
        self.assertIn("value", mismatches)

    def test_validate_column_types_raises_exception(self):
        """Test that validate_column_types raises exception on type mismatch"""
        schema = StructType([
            StructField("id", IntegerType(), nullable=False)
        ])

        df = self.spark.createDataFrame([(1,)], schema)

        with self.assertRaises(SchemaValidationError):
            SchemaValidator.validate_column_types(
                df,
                {"id": StringType()},
                raise_on_error=True
            )

    def test_complex_type_matching(self):
        """Test that complex types (arrays, structs) are matched correctly"""
        schema_with_array = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("tags", ArrayType(StringType()), nullable=True)
        ])

        df = self.spark.createDataFrame(
            [(1, ["tag1", "tag2"]), (2, ["tag3"])],
            schema_with_array
        )

        # Should match
        is_valid, errors = SchemaValidator.validate_schema(
            df, schema_with_array, raise_on_error=False
        )
        self.assertTrue(is_valid)

        # Should not match with different array element type
        wrong_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("tags", ArrayType(IntegerType()), nullable=True)
        ])

        is_valid, errors = SchemaValidator.validate_schema(
            df, wrong_schema, raise_on_error=False
        )
        self.assertFalse(is_valid)

    def test_validate_bsm_raw_schema(self):
        """Test convenience function for BSM raw schema validation"""
        # Create a minimal DataFrame with BSM raw schema
        schema = get_bsm_raw_schema()

        # Create a sample row with correct types (19 columns)
        sample_data = [(
            "07/31/2019 12:41:59 PM",  # metadata_generatedAt
            "BSM",                      # metadata_recordType
            "stream1",                  # metadata_serialId_streamId
            100,                        # metadata_serialId_bundleSize
            1,                          # metadata_serialId_bundleId
            1,                          # metadata_serialId_recordId
            12345,                      # metadata_serialId_serialNumber
            "07/31/2019 12:42:00 PM",  # metadata_receivedAt
            "0x1a2b3c4d",              # coreData_id
            41590,                      # coreData_secMark
            41.25901,                   # coreData_position_lat
            -104.98765,                 # coreData_position_long
            12.5,                       # coreData_accuracy_semiMajor
            11.0,                       # coreData_accuracy_semiMinor
            1655.5,                     # coreData_elevation
            0.5,                        # coreData_accelset_accelYaw
            15.5,                       # coreData_speed
            180.0,                      # coreData_heading
            "POINT (-104.98765 41.25901)"  # coreData_position
        )]

        df = self.spark.createDataFrame(sample_data, schema)

        # Should validate successfully
        is_valid, errors = validate_bsm_raw_schema(df, raise_on_error=False)
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)

    def test_validate_bsm_processed_schema(self):
        """Test convenience function for BSM processed schema validation"""
        # Create a minimal DataFrame with BSM processed schema
        schema = get_bsm_processed_schema()

        # Create a sample row with correct types (18 columns)
        sample_data = [(
            123456,         # coreData_id (converted from hex to decimal)
            41590,          # coreData_secMark
            12.5,           # coreData_accuracy_semiMajor
            11.0,           # coreData_accuracy_semiMinor
            1655.5,         # coreData_elevation
            0.5,            # coreData_accelset_accelYaw
            15.5,           # coreData_speed
            180.0,          # coreData_heading
            1234.56,        # x_pos
            5678.90,        # y_pos
            7,              # month
            31,             # day
            2019,           # year
            12,             # hour
            41,             # minute
            59,             # second
            1,              # pm (1 = PM)
            0               # isAttacker (0 = legitimate)
        )]

        df = self.spark.createDataFrame(sample_data, schema)

        # Should validate successfully
        is_valid, errors = validate_bsm_processed_schema(df, raise_on_error=False)
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)


if __name__ == '__main__':
    unittest.main()
