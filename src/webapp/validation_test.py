import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch
from src.webapp.validation import validate_file_reader, HardValidationError

# Minimal schema for testing
MOCK_BASE_SCHEMA = {
    "base": {
        "data_models": {
            "test_model": {
                "columns": {
                    "foo_col": {
                        "dtype": "int",
                        "nullable": False,
                        "required": True,
                        "aliases": ["foo"]
                    },
                    "bar_col": {
                        "dtype": "str",
                        "nullable": True,
                        "required": False,
                        "aliases": ["bar"]
                    }
                }
            }
        }
    }
}

MOCK_EXT_SCHEMA = {
    "institutions": {
        "pdp": {
            "data_models": {}
        }
    }
}


@pytest.fixture
def tmp_csv_file(tmp_path: Path):
    df = pd.DataFrame({"foo_col": [1, 2], "bar_col": ["a", "b"]})
    file_path = tmp_path / "test.csv"
    df.to_csv(file_path, index=False)
    return str(file_path)


def test_validate_file_reader_passes(tmp_csv_file):
    with patch("src.webapp.validation.load_json") as mock_load, patch("os.path.exists", return_value=True):
        mock_load.side_effect = lambda path: MOCK_BASE_SCHEMA if "base" in path else MOCK_EXT_SCHEMA
        result = validate_file_reader(tmp_csv_file, ["test_model"])
        assert result["validation_status"] == "passed"
        assert result["schemas"] == ["test_model"]


def test_validate_file_reader_fails_missing_required(tmp_path):
    df = pd.DataFrame({"bar_col": ["x", "y"]})  # Missing "foo_col"
    file_path = tmp_path / "invalid.csv"
    df.to_csv(file_path, index=False)

    with patch("src.webapp.validation.load_json") as mock_load, patch("os.path.exists", return_value=True):
        mock_load.side_effect = lambda path: MOCK_BASE_SCHEMA if "base" in path else MOCK_EXT_SCHEMA
        with pytest.raises(HardValidationError) as exc_info:
            validate_file_reader(str(file_path), ["test_model"])
        assert "Missing required columns" in str(exc_info.value)
