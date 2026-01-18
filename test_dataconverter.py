"""Test DataConverter directly."""

from Helpers.DataConverter import DataConverter

test_points = [
    "POINT(-105.479416 39.374540)",
    "POINT(-104.530090 39.950714)",
    "POINT (-104.667557 39.731994)",  # With space after POINT
]

print("Testing DataConverter.point_to_tuple():")
print("="*50)

for point_str in test_points:
    try:
        result = DataConverter.point_to_tuple(point_str)
        print(f"{point_str:45} -> {result}")
    except Exception as e:
        print(f"{point_str:45} -> ERROR: {e}")
