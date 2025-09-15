"""Test file for the gcsdbutils.py file functions."""

from .gcsdbutils import get_job_id


def test_get_job_id():
    """From a fully qualified file name (i.e. everything sub-bucket name level), get the job id."""
    assert get_job_id("approved/123445/inference_output.csv") == 123445
