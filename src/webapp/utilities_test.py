"""Test file for utilities.py."""

import pytest

from fastapi import HTTPException
from .utilities import (
    decode_url_piece,
    display_model_name,
    uc_model_name,
    expand_batch_file_name_lookups,
    file_name_variants_for_lookup,
    has_access_to_inst_or_err,
    has_full_data_access_or_err,
    has_at_most_one_school_type,
    uuid_to_str,
    databricksify_inst_name,
)

from .test_helper import USR, DATAKINDER, VIEWER, UUID_INVALID, USER_VALID_INST_UUID


def test_base_user_class_functions():
    """Run tests on various BaseUser class functions."""
    assert DATAKINDER.is_datakinder()
    assert not USR.is_datakinder()

    assert DATAKINDER.has_access_to_inst(uuid_to_str(USER_VALID_INST_UUID))
    assert USR.has_access_to_inst(uuid_to_str(USER_VALID_INST_UUID))
    assert not USR.has_access_to_inst(uuid_to_str(UUID_INVALID))

    assert DATAKINDER.has_full_data_access()
    assert USR.has_full_data_access()
    assert not VIEWER.has_full_data_access()


def test_has_at_most_one_school_type() -> None:
    """Test mutual exclusivity helper: at most one school-type id may be set."""
    assert has_at_most_one_school_type(None, None, None, None) is True
    assert has_at_most_one_school_type("pdp1", None, None, None) is True
    assert has_at_most_one_school_type(None, "edvise1", None, None) is True
    assert has_at_most_one_school_type(None, None, "legacy1", None) is True
    assert has_at_most_one_school_type(None, None, None, "genai1") is True
    assert has_at_most_one_school_type("pdp1", "edvise1", None, None) is False
    assert has_at_most_one_school_type("pdp1", None, "legacy1", None) is False
    assert has_at_most_one_school_type(None, "edvise1", "legacy1", None) is False
    assert has_at_most_one_school_type("pdp1", "edvise1", "legacy1", None) is False
    assert has_at_most_one_school_type(None, None, "legacy1", "genai1") is False


def test_has_access_to_inst_or_err():
    """Testing valid check for access to institution."""
    with pytest.raises(HTTPException) as err:
        has_access_to_inst_or_err("456", USR)
    assert err.value.status_code == 401
    assert err.value.detail == "Not authorized to read this institution's resources."


def test_has_full_data_access_or_err():
    """Testing valid check for access to full data."""
    with pytest.raises(HTTPException) as err:
        has_full_data_access_or_err(VIEWER, "models")
    assert err.value.status_code == 401
    assert err.value.detail == "Not authorized to view models for this institution."


def test_databricksify_inst_name():
    """
    Testing databricksifying institution name
    """
    assert (
        databricksify_inst_name("The University of Mildly Impressive Achievements")
        == "the_uni_of_mildly_impressive_achievements"
    )
    assert (
        databricksify_inst_name("Dandelion Technical & Tractor College")
        == "dandelion_technical_tractor_col"
    )
    assert (
        databricksify_inst_name("Fernwood & Finch Academy") == "fernwood_finch_academy"
    )
    assert (
        databricksify_inst_name("The Center for Applied Napping")
        == "the_center_for_applied_napping"
    )
    assert (
        databricksify_inst_name("Harrisville University of Science and Technology")
        == "harrisville_uni_st"
    )
    assert (
        databricksify_inst_name("University of Questionable Decisions")
        == "uni_of_questionable_decisions"
    )
    assert (
        databricksify_inst_name("Badger Hollow University of Science & Scones")
        == "badger_hollow_uni_of_science_scones"
    )

    with pytest.raises(ValueError) as err:
        databricksify_inst_name("Northwest (invalid)")
    assert str(err.value) == "Unexpected character found in Databricks compatible name."


def test_decode_url_piece_treats_plus_as_space() -> None:
    """Form-style + encoding in paths should decode to spaces."""
    assert decode_url_piece("a+b.csv") == "a b.csv"
    assert decode_url_piece("foo%20bar.csv") == "foo bar.csv"
    assert decode_url_piece("x%2By.csv") == "x+y.csv"
    assert (
        decode_url_piece("graduation_in_3y_ft_4.5y_pt_checkpoint_30_credits")
        == "graduation_in_3y_ft_4d5y_pt_checkpoint_30_credits"
    )
    assert (
        decode_url_piece("graduation_in_3y_ft_4.5Y_pt_checkpoint_30_credits")
        == "graduation_in_3y_ft_4d5y_pt_checkpoint_30_credits"
    )
    assert decode_url_piece("1.2.csv") == "1.2.csv"


def test_display_model_name_decodes_uc_decimals() -> None:
    """UC stores 4d5y; the frontend should see 4.5Y to match compact 3Y."""
    assert (
        display_model_name("graduation_in_3y_ft_4d5y_pt_checkpoint_30_credits")
        == "graduation_in_3y_ft_4.5Y_pt_checkpoint_30_credits"
    )
    assert display_model_name("graduation_in_2d5y") == "graduation_in_2.5Y"
    assert (
        display_model_name("sample_model_for_school_1") == "sample_model_for_school_1"
    )


def test_uc_model_name_encodes_decimal_time_limits() -> None:
    """Unity Catalog rejects `.`; encode 4.5y as 4d5y at the Databricks boundary."""
    assert (
        uc_model_name("graduation_in_3y_ft_4.5y_pt_checkpoint_30_credits")
        == "graduation_in_3y_ft_4d5y_pt_checkpoint_30_credits"
    )
    assert (
        uc_model_name("graduation_in_3y_ft_4.5Y_pt_checkpoint_30_credits")
        == "graduation_in_3y_ft_4d5y_pt_checkpoint_30_credits"
    )
    assert (
        uc_model_name("graduation_in_3y_ft_4d5y_pt_checkpoint_30_credits")
        == "graduation_in_3y_ft_4d5y_pt_checkpoint_30_credits"
    )
    assert uc_model_name("sample_model_for_school_1") == "sample_model_for_school_1"


def test_file_name_variants_for_lookup() -> None:
    """Batch lookups accept spaces vs. plus spellings."""
    v = file_name_variants_for_lookup("a b.csv")
    assert v == {"a b.csv", "a+b.csv"}
    assert file_name_variants_for_lookup("  a+b.csv  ") == {"a b.csv", "a+b.csv"}


def test_expand_batch_file_name_lookups() -> None:
    out = set(expand_batch_file_name_lookups(["x y.csv", "p+q.csv"]))
    assert "x y.csv" in out and "x+y.csv" in out
    assert "p q.csv" in out and "p+q.csv" in out
