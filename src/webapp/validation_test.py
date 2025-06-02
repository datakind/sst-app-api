"""Test file for file_validation.py."""

import pytest

from .validation import (
    valid_subset_lists,
    detect_file_type,
    get_col_names,
    validate_file,
    SchemaType,
)


def test_get_col_names():
    """Testing getting the column names."""
    with open("src/webapp/test_files/test_upload.csv", encoding="utf-8") as f:
        cols = get_col_names(f)
        assert cols == ["foo_col", "bar_col", "baz_col"]


def test_valid_subset_lists():
    list_a = ["a", "b", "c", "d"]
    list_b = ["a", "b", "c"]
    optional = ["d"]

    result1 = valid_subset_lists(list_a, list_b, optional)
    assert result1.is_valid
    assert result1.unexpected_columns == []
    assert result1.missing_required_columns == []

    result2 = valid_subset_lists(list_a, list_b, [])
    assert not result2.is_valid
    assert result2.missing_required_columns == ["d"]

    result3 = valid_subset_lists(list_b, list_a, [])
    assert not result3.is_valid
    assert result3.unexpected_columns == ["d"]

def test_detect_file_type():
    with open("src/webapp/test_files/financial_sst_pdp.csv", encoding="utf-8") as f:
        cols = get_col_names(f)
        assert detect_file_type(cols, {SchemaType.SST_PDP_FINANCE}) == {SchemaType.SST_PDP_FINANCE}

    with open("src/webapp/test_files/course_sst_pdp.csv", encoding="utf-8") as f:
        cols = get_col_names(f)
        assert detect_file_type(cols, {SchemaType.SST_PDP_COURSE}) == {SchemaType.SST_PDP_COURSE}

    with open("src/webapp/test_files/cohort_sst_pdp.csv", encoding="utf-8") as f:
        cols = get_col_names(f)
        assert detect_file_type(cols, {SchemaType.SST_PDP_COHORT}) == {SchemaType.SST_PDP_COHORT}

    with open("src/webapp/test_files/test_upload.csv", encoding="utf-8") as f:
        cols = get_col_names(f)
        with pytest.raises(ValueError) as err:
            detect_file_type(cols, {SchemaType.SST_PDP_COHORT, SchemaType.SST_PDP_COURSE})
        assert "Required file schema(s) not recognized" in str(err.value)
        assert "Unexpected columns" in str(err.value)

def test_malformed_csv():
    with open("src/webapp/test_files/malformed.csv", encoding="utf-8") as f:
        with pytest.raises(ValueError) as err:
            get_col_names(f)
        assert "CSV file malformed" in str(err.value)

def test_validate_file():
    # Finance should validate on its own
    assert validate_file(
        "src/webapp/test_files/financial_sst_pdp.csv",
        {SchemaType.SST_PDP_FINANCE}
    )

    # Course should validate
    assert validate_file(
        "src/webapp/test_files/course_sst_pdp.csv",
        {SchemaType.SST_PDP_COURSE}
    )

    # Cohort should validate
    assert validate_file(
        "src/webapp/test_files/cohort_sst_pdp.csv",
        {SchemaType.SST_PDP_COHORT}
    )

    # File with only junk columns should fail
    with pytest.raises(ValueError) as err:
        validate_file(
            "src/webapp/test_files/test_upload.csv",
            {SchemaType.SST_PDP_COHORT, SchemaType.SST_PDP_COURSE, SchemaType.SST_PDP_FINANCE}
        )
    assert "Required file schema(s) not recognized" in str(err.value)
    assert "Unexpected columns" in str(err.value)

    # A valid file with only finance optional fields and no Pell should fail
    with open("src/webapp/test_files/finance_missing_pell.csv", encoding="utf-8") as f:
        cols = get_col_names(f)
        with pytest.raises(ValueError) as err:
            detect_file_type(cols, {SchemaType.SST_PDP_FINANCE})
        assert "Missing required columns" in str(err.value)