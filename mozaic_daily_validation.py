"""Compatibility shim - import from mozaic_daily.validation instead."""
import sys
from pathlib import Path

src_path = Path(__file__).parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from mozaic_daily.validation import *
from mozaic_daily.config import get_constants

if __name__ == '__main__':
    import pandas as pd
    df = pd.read_parquet(get_constants()['forecast_checkpoint_filename'])
    validate_output_dataframe(df)
