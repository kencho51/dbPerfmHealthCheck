"""
Unit tests for api/services/ingestor._derive_month_year.

Tests cover:
  TestTimezoneAwareTimestamps — +08:00 / +0800 / -05:00 offsets stripped correctly
  TestNaiveTimestamps         — standard ISO / slashed / MM/DD/YYYY patterns
  TestEdgeCases               — None, empty, gibberish, whitespace inputs

Run:
    uv run pytest tests/test_derive_month_year.py -v
"""

from __future__ import annotations

import pytest

from api.services.ingestor import _derive_month_year

# ---------------------------------------------------------------------------
# Timezone-aware timestamps (the main regression class fixed in this session)
# ---------------------------------------------------------------------------


class TestTimezoneAwareTimestamps:
    """Splunk exports timestamps with +08:00 or +0800 offsets.
    Before the fix, strptime raised ValueError on all these; they all returned None."""

    @pytest.mark.parametrize(
        "ts, expected",
        [
            # +HH:MM colon-separated
            ("2026-02-28T23:55:18.000+08:00", "2026-02"),
            ("2026-11-01T10:00:00.000+08:00", "2026-11"),
            ("2025-12-31T23:59:59.999+08:00", "2025-12"),
            ("2026-01-15T12:30:00+05:30", "2026-01"),
            # -HH:MM negative offset
            ("2026-03-01T00:00:00.000-05:00", "2026-03"),
            # +HHMM no colon
            ("2026-02-28T23:55:18.000+0800", "2026-02"),
            ("2026-02-28T23:55:18+0800", "2026-02"),
            # -HHMM no colon
            ("2026-09-15T08:00:00.000-0500", "2026-09"),
        ],
    )
    def test_timezone_aware_parsed(self, ts: str, expected: str):
        assert _derive_month_year(ts) == expected

    def test_plus_zero_offset(self):
        # +0000 / UTC+0 should work too
        assert _derive_month_year("2026-06-01T00:00:00.000+0000") == "2026-06"

    def test_offset_stripped_not_confused_with_date(self):
        # Ensure the regex only strips the trailing offset, not the date
        result = _derive_month_year("2026-04-30T22:00:00.000+0800")
        assert result == "2026-04", f"unexpected: {result!r}"


# ---------------------------------------------------------------------------
# Standard naive timestamps (should still work after the regex was added)
# ---------------------------------------------------------------------------


class TestNaiveTimestamps:
    @pytest.mark.parametrize(
        "ts, expected",
        [
            # ISO 8601 with sub-seconds
            ("2025-11-15T09:30:00.123", "2025-11"),
            # ISO 8601 no sub-seconds
            ("2025-11-15T09:30:00", "2025-11"),
            # Space-separated
            ("2025-11-15 09:30:00.123", "2025-11"),
            ("2025-11-15 09:30:00", "2025-11"),
            # Date only
            ("2025-11-15", "2025-11"),
            # Slashed year-first
            ("2025/11/15 09:30:00", "2025-11"),
            # US format M/D/Y
            ("11/15/2025 09:30:00 AM", "2025-11"),
            ("02/28/2026 11:55:18 PM", "2026-02"),
        ],
    )
    def test_standard_formats_parsed(self, ts: str, expected: str):
        assert _derive_month_year(ts) == expected

    def test_first_day_of_month(self):
        assert _derive_month_year("2026-01-01T00:00:00") == "2026-01"

    def test_last_day_of_year(self):
        assert _derive_month_year("2025-12-31T23:59:59") == "2025-12"


# ---------------------------------------------------------------------------
# Edge cases — should all return None cleanly, never raise
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_none_returns_none(self):
        assert _derive_month_year(None) is None

    def test_empty_string_returns_none(self):
        assert _derive_month_year("") is None

    def test_whitespace_only_returns_none(self):
        assert _derive_month_year("   ") is None

    def test_gibberish_returns_none(self):
        assert _derive_month_year("not-a-date") is None

    def test_just_year_returns_none(self):
        # No day/month part — should not produce a spurious match
        assert _derive_month_year("2026") is None

    def test_partial_date_returns_none(self):
        assert _derive_month_year("2026-02") is None

    def test_does_not_raise_on_any_string(self):
        """_derive_month_year must never raise even for arbitrary garbage."""
        for s in ["???", "SELECT 1", "Jan 2026", "2026/02", "16:04:00", "abc+0800"]:
            try:
                _derive_month_year(s)  # result doesn't matter, just no exception
            except Exception as exc:  # noqa: BLE001
                pytest.fail(f"_derive_month_year({s!r}) raised {exc!r}")
