#!/usr/bin/env python
"""Test script for validating Docker image functionality."""
import sys
import pandas as pd

# Add package to path
sys.path.insert(0, '/src')

# Import from mozaic_daily package
from mozaic_daily import main
from mozaic_daily.validation import validate_output_dataframe

def test_docker_image():
    """Run main function and validate output."""
    print("=" * 80)
    print("DOCKER IMAGE TEST")
    print("=" * 80)

    try:
        print("\n[1/3] Running main forecast function...")
        print("Note: This requires BigQuery credentials or checkpoint files")

        df = main(project="moz-fx-data-bq-data-science", testing_mode='ENABLE_TESTING_MODE')
        print(f"✓ Forecast generated: {len(df)} rows")

        print("\n[2/3] Running validation...")
        validate_output_dataframe(df, testing_mode=True)
        print("✓ Validation passed")

        print("\n[3/3] Sample output:")
        print("-" * 80)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(df.head(10))
        print(f"\n... ({len(df)} total rows)")
        print("-" * 80)

        print("\n" + "=" * 80)
        print("✓ DOCKER IMAGE TEST PASSED")
        print("=" * 80)
        return 0

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        print("\n" + "=" * 80)
        print("✗ DOCKER IMAGE TEST FAILED")
        print("=" * 80)
        return 1

if __name__ == "__main__":
    sys.exit(test_docker_image())
