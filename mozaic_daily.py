"""Compatibility shim - import from mozaic_daily package instead.

This file acts as a shim to maintain backward compatibility. The actual
implementation has been moved to the mozaic_daily package in src/.
"""
import sys
from pathlib import Path

# Add src to path so we can import the mozaic_daily package
src_path = Path(__file__).parent / "src"
src_path_str = str(src_path)
if src_path_str not in sys.path:
    sys.path.insert(0, src_path_str)

# Temporarily remove this module from sys.modules if it's there
# This prevents circular import issues
_self_module = sys.modules.get('mozaic_daily')
if _self_module is not None and hasattr(_self_module, '__file__') and _self_module.__file__ == __file__:
    del sys.modules['mozaic_daily']

# Now import the actual package
import mozaic_daily as _pkg

# Import additional functions from submodules and add them to the package
from mozaic_daily.data import (
    desktop_query, mobile_query, get_queries, get_aggregate_data
)
from mozaic_daily.forecast import (
    get_forecast_dfs, get_desktop_forecast_dfs, get_mobile_forecast_dfs
)
from mozaic_daily.tables import (
    combine_tables, update_desktop_format, update_mobile_format,
    add_desktop_and_mobile_rows, format_output_table,
    get_git_commit_hash, get_git_commit_hash_from_pip, get_git_commit_hash_from_file
)

# Add these functions to the package for backward compatibility
_pkg.desktop_query = desktop_query
_pkg.mobile_query = mobile_query
_pkg.get_queries = get_queries
_pkg.get_aggregate_data = get_aggregate_data
_pkg.get_forecast_dfs = get_forecast_dfs
_pkg.get_desktop_forecast_dfs = get_desktop_forecast_dfs
_pkg.get_mobile_forecast_dfs = get_mobile_forecast_dfs
_pkg.combine_tables = combine_tables
_pkg.update_desktop_format = update_desktop_format
_pkg.update_mobile_format = update_mobile_format
_pkg.add_desktop_and_mobile_rows = add_desktop_and_mobile_rows
_pkg.format_output_table = format_output_table
_pkg.get_git_commit_hash = get_git_commit_hash
_pkg.get_git_commit_hash_from_pip = get_git_commit_hash_from_pip
_pkg.get_git_commit_hash_from_file = get_git_commit_hash_from_file

# Convenience alias
main = _pkg.main

# Replace this module with the package in sys.modules
sys.modules['mozaic_daily'] = sys.modules[__name__] = _pkg

if __name__ == '__main__':
    _pkg.main(checkpoints=True)
