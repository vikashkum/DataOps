"""
test_load.py
~~~~~~~~~~~~
Unit tests for scripts/load.py Gold-layer builders.
No database connection required — tests the pure-function data transformations.
"""

from scripts.load import build_dim_date


class TestBuildDimDate:
    def test_row_count(self):
        df = build_dim_date("2024-01-01", "2024-03-31")
        assert len(df) == 91  # Jan(31) + Feb(29) + Mar(31) = 91

    def test_date_key_format(self):
        df = build_dim_date("2024-06-15", "2024-06-15")
        assert df.iloc[0]["date_key"] == 20240615

    def test_no_duplicate_date_keys(self):
        df = build_dim_date("2023-01-01", "2023-12-31")
        assert df["date_key"].nunique() == len(df)

    def test_weekend_flag(self):
        # 2024-01-06 is a Saturday
        df = build_dim_date("2024-01-06", "2024-01-07")
        saturday = df[df["date_key"] == 20240106].iloc[0]
        sunday = df[df["date_key"] == 20240107].iloc[0]
        assert saturday["is_weekend"] is True or saturday["is_weekend"] == True   # noqa: E712
        assert sunday["is_weekend"] is True or sunday["is_weekend"] == True       # noqa: E712

    def test_weekday_not_weekend(self):
        # 2024-01-08 is a Monday
        df = build_dim_date("2024-01-08", "2024-01-08")
        monday = df.iloc[0]
        assert monday["is_weekend"] is False or monday["is_weekend"] == False   # noqa: E712

    def test_required_columns(self):
        df = build_dim_date("2024-01-01", "2024-01-01")
        required = {"date_key", "full_date", "day", "month", "year",
                    "quarter", "day_of_week", "day_name", "month_name", "is_weekend"}
        assert required.issubset(set(df.columns))

    def test_quarter_values(self):
        df = build_dim_date("2024-01-01", "2024-12-31")
        assert set(df["quarter"].unique()) == {1, 2, 3, 4}
