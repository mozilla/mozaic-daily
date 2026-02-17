"""Tests for scripts/run_flow.py"""

import json
import subprocess
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

# Add scripts directory to path so we can import run_flow
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

import run_flow


class TestDateUtilities:
    """Test date generation and filtering functions."""

    def test_generate_date_range_basic(self):
        """Test basic date range generation."""
        dates = run_flow.generate_date_range("2024-06-01", "2024-06-05")
        assert dates == [
            "2024-06-01",
            "2024-06-02",
            "2024-06-03",
            "2024-06-04",
            "2024-06-05",
        ]

    def test_generate_date_range_single_day(self):
        """Test single day range."""
        dates = run_flow.generate_date_range("2024-06-15", "2024-06-15")
        assert dates == ["2024-06-15"]

    def test_generate_date_range_invalid_order(self):
        """Test error when start is after end."""
        with pytest.raises(ValueError, match="Start date .* is after end date"):
            run_flow.generate_date_range("2024-06-30", "2024-06-01")

    def test_filter_dates_by_weekday_single(self):
        """Test filtering to a single weekday."""
        dates = run_flow.generate_date_range("2025-07-01", "2025-07-07")
        # July 1, 2025 is a Tuesday
        mondays = run_flow.filter_dates_by_weekday(dates, ["monday"])
        assert mondays == ["2025-07-07"]  # July 7 is Monday

    def test_filter_dates_by_weekday_multiple(self):
        """Test filtering to multiple weekdays."""
        dates = run_flow.generate_date_range("2025-07-01", "2025-07-07")
        # July 1, 2025 is a Tuesday, July 6 is Sunday
        weekend = run_flow.filter_dates_by_weekday(dates, ["saturday", "sunday"])
        assert weekend == ["2025-07-05", "2025-07-06"]

    def test_filter_dates_by_weekday_empty_result(self):
        """Test filtering that results in empty list."""
        dates = ["2025-07-01"]  # Tuesday
        mondays = run_flow.filter_dates_by_weekday(dates, ["monday"])
        assert mondays == []

    def test_filter_dates_no_weekdays(self):
        """Test filtering with no weekdays returns all dates."""
        dates = ["2025-07-01", "2025-07-02"]
        result = run_flow.filter_dates_by_weekday(dates, None)
        assert result == dates


class TestSubprocessRunner:
    """Test the subprocess runner utility."""

    @patch("run_flow.subprocess.run")
    def test_run_flow_subprocess_local_mode(self, mock_run):
        """Test subprocess runner in local mode sets environment variable."""
        mock_run.return_value = Mock(returncode=0)

        run_flow.run_flow_subprocess(["--test"], local_mode=True)

        # Check that subprocess.run was called with correct args
        call_args = mock_run.call_args
        assert call_args[0][0] == ["python", "mozaic_daily_flow.py", "run", "--test"]
        assert call_args[1]["env"]["METAFLOW_LOCAL_MODE"] == "true"

    @patch("run_flow.subprocess.run")
    def test_run_flow_subprocess_remote_mode(self, mock_run):
        """Test subprocess runner in remote mode doesn't set env var."""
        mock_run.return_value = Mock(returncode=0)

        run_flow.run_flow_subprocess(["--test"], local_mode=False)

        # Check that METAFLOW_LOCAL_MODE is not in env
        call_args = mock_run.call_args
        assert call_args[0][0] == ["python", "mozaic_daily_flow.py", "run", "--test"]
        assert "METAFLOW_LOCAL_MODE" not in call_args[1]["env"]

    @patch("run_flow.subprocess.run")
    def test_run_flow_subprocess_extra_args(self, mock_run):
        """Test subprocess runner appends extra arguments."""
        mock_run.return_value = Mock(returncode=0)

        run_flow.run_flow_subprocess(["--arg1", "value1", "--arg2"], local_mode=False)

        # Check command includes all args
        call_args = mock_run.call_args
        assert call_args[0][0] == [
            "python",
            "mozaic_daily_flow.py",
            "run",
            "--arg1",
            "value1",
            "--arg2",
        ]


class TestSingleBackfill:
    """Test single backfill execution."""

    @patch("run_flow.run_flow_subprocess")
    def test_single_backfill_creates_log_file(self, mock_subprocess, tmp_path):
        """Test that single backfill creates a log file."""
        mock_subprocess.return_value = Mock(
            returncode=0, stdout="test output", stderr=""
        )

        date, success, log_file = run_flow.run_single_backfill(
            "2024-06-15", tmp_path, local_mode=False
        )

        assert date == "2024-06-15"
        assert success is True
        assert Path(log_file).exists()
        assert "backfill_2024-06-15.log" in log_file

    @patch("run_flow.run_flow_subprocess")
    def test_single_backfill_local_mode_propagation(self, mock_subprocess, tmp_path):
        """Test that local_mode is passed through to subprocess."""
        mock_subprocess.return_value = Mock(
            returncode=0, stdout="test output", stderr=""
        )

        run_flow.run_single_backfill("2024-06-15", tmp_path, local_mode=True)

        # Check that local_mode=True was passed
        call_args = mock_subprocess.call_args
        assert call_args[1]["local_mode"] is True

    @patch("run_flow.run_flow_subprocess")
    def test_single_backfill_timeout_handling(self, mock_subprocess, tmp_path):
        """Test that timeout is set correctly."""
        mock_subprocess.side_effect = subprocess.TimeoutExpired("cmd", 14400)

        date, success, log_file = run_flow.run_single_backfill(
            "2024-06-15", tmp_path, local_mode=False
        )

        assert date == "2024-06-15"
        assert success is False
        assert Path(log_file).exists()


class TestStateManagement:
    """Test backfill state management."""

    def test_load_state_nonexistent_file(self, tmp_path):
        """Test loading state from nonexistent file returns empty state."""
        state_file = tmp_path / "nonexistent.json"
        state = run_flow.load_backfill_state(state_file)

        assert state == {"completed_dates": [], "failed_dates": []}

    def test_load_state_existing_file(self, tmp_path):
        """Test loading state from existing file."""
        state_file = tmp_path / "state.json"
        expected_state = {
            "start_date": "2024-06-01",
            "end_date": "2024-06-30",
            "completed_dates": ["2024-06-01", "2024-06-02"],
            "failed_dates": [],
        }
        with open(state_file, "w") as f:
            json.dump(expected_state, f)

        state = run_flow.load_backfill_state(state_file)
        assert state == expected_state

    def test_save_state_creates_file(self, tmp_path):
        """Test saving state creates file."""
        state_file = tmp_path / "state.json"
        state = {
            "start_date": "2024-06-01",
            "end_date": "2024-06-30",
            "completed_dates": ["2024-06-01"],
            "failed_dates": [],
        }

        run_flow.save_backfill_state(state_file, state)

        assert state_file.exists()
        with open(state_file, "r") as f:
            loaded = json.load(f)
        assert loaded["start_date"] == "2024-06-01"
        assert loaded["completed_dates"] == ["2024-06-01"]
        assert "updated_at" in loaded

    def test_save_state_updates_timestamp(self, tmp_path):
        """Test saving state updates timestamp."""
        state_file = tmp_path / "state.json"
        state = {"completed_dates": [], "failed_dates": []}

        run_flow.save_backfill_state(state_file, state)

        with open(state_file, "r") as f:
            loaded = json.load(f)
        assert "updated_at" in loaded
        # Check it's a valid ISO timestamp
        datetime.fromisoformat(loaded["updated_at"])

    def test_state_file_path_generation(self, tmp_path):
        """Test state file path generation."""
        path = run_flow.get_state_file_path(
            tmp_path, "2024-06-01", "2024-06-30", None
        )
        assert path == tmp_path / "backfill_state_2024-06-01_2024-06-30.json"

    def test_state_file_path_with_weekdays(self, tmp_path):
        """Test state file path generation with weekdays."""
        path = run_flow.get_state_file_path(
            tmp_path, "2024-06-01", "2024-06-30", ["monday", "friday"]
        )
        assert path == tmp_path / "backfill_state_2024-06-01_2024-06-30_friday_monday.json"


class TestResume:
    """Test resume functionality."""

    @patch("run_flow.run_single_backfill")
    def test_resume_skips_completed_dates(self, mock_backfill, tmp_path):
        """Test that resume skips previously completed dates."""
        mock_backfill.return_value = ("2024-06-03", True, "log.txt")

        # Create state file with completed dates
        state_file = run_flow.get_state_file_path(
            tmp_path, "2024-06-01", "2024-06-03", None
        )
        state = {
            "start_date": "2024-06-01",
            "end_date": "2024-06-03",
            "completed_dates": ["2024-06-01", "2024-06-02"],
            "failed_dates": [],
        }
        run_flow.save_backfill_state(state_file, state)

        # Run backfill with resume
        with patch("run_flow.Path", return_value=tmp_path):
            exit_code = run_flow.run_backfill(
                "2024-06-01",
                "2024-06-03",
                parallel=1,
                resume=True,
                local_mode=True,
            )

        # Should only process 2024-06-03
        assert mock_backfill.call_count == 1
        assert mock_backfill.call_args[0][0] == "2024-06-03"

    @patch("run_flow.run_single_backfill")
    def test_resume_with_no_prior_state(self, mock_backfill, tmp_path):
        """Test that resume with no prior state runs all dates."""
        mock_backfill.return_value = ("2024-06-01", True, "log.txt")

        # Run backfill with resume but no existing state
        with patch("run_flow.Path", return_value=tmp_path):
            exit_code = run_flow.run_backfill(
                "2024-06-01",
                "2024-06-01",
                parallel=1,
                resume=True,
                local_mode=True,
            )

        # Should process the date
        assert mock_backfill.call_count == 1


class TestDryRun:
    """Test dry run functionality."""

    def test_dry_run_prints_without_running(self, capsys):
        """Test that dry run prints dates without running backfill."""
        exit_code = run_flow.run_backfill(
            "2024-06-01",
            "2024-06-03",
            parallel=1,
            dry_run=True,
            local_mode=False,
        )

        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out
        assert "2024-06-01" in captured.out
        assert "2024-06-02" in captured.out
        assert "2024-06-03" in captured.out
        assert "Execution mode: remote" in captured.out
        assert exit_code == 0

    def test_dry_run_respects_weekday_filter(self, capsys):
        """Test that dry run respects weekday filtering."""
        exit_code = run_flow.run_backfill(
            "2025-07-01",
            "2025-07-07",
            parallel=1,
            weekdays=["monday"],
            dry_run=True,
            local_mode=False,
        )

        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out
        assert "2025-07-07" in captured.out  # Monday
        assert "Monday" in captured.out
        # Should not have other days
        assert "2025-07-01" in captured.out or "Tuesday" not in captured.out
        assert exit_code == 0


class TestBackfillSummary:
    """Test backfill summary printing."""

    def test_print_backfill_summary_success(self, capsys):
        """Test summary printing for successful run."""
        run_flow.print_backfill_summary(
            total=3,
            succeeded=["2024-06-01", "2024-06-02", "2024-06-03"],
            failed=[],
        )

        captured = capsys.readouterr()
        assert "BACKFILL SUMMARY" in captured.out
        assert "Total: 3" in captured.out
        assert "Succeeded: 3" in captured.out
        assert "Failed: 0" in captured.out
        assert "All backfills completed successfully!" in captured.out

    def test_print_backfill_summary_with_failures(self, capsys):
        """Test summary printing with failures."""
        run_flow.print_backfill_summary(
            total=3, succeeded=["2024-06-01"], failed=["2024-06-02", "2024-06-03"]
        )

        captured = capsys.readouterr()
        assert "BACKFILL SUMMARY" in captured.out
        assert "Total: 3" in captured.out
        assert "Succeeded: 1" in captured.out
        assert "Failed: 2" in captured.out
        assert "Failed dates:" in captured.out
        assert "2024-06-02" in captured.out
        assert "2024-06-03" in captured.out
