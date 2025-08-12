import pytest

from .databricks import DatabricksControl

@pytest.fixture
def ctrl():
    return DatabricksControl()

def test_exact_literal_case_insensitive(ctrl):
    mapping = {"student": "student.csv"}
    assert ctrl.get_key_for_file(mapping, "Student.csv") == "student"

def test_literal_with_suffix_and_same_ext(ctrl):
    mapping = {"student": "student.csv"}
    assert ctrl.get_key_for_file(mapping, "student_20240101.csv") == "student"
    assert ctrl.get_key_for_file(mapping, "student-final.csv") == "student"
    # should not match a different extension
    assert ctrl.get_key_for_file(mapping, "student_20240101.tsv") is None

def test_literal_without_ext_allows_suffix_and_optional_ext(ctrl):
    mapping = {"student": "student"}
    assert ctrl.get_key_for_file(mapping, "student") == "student"
    assert ctrl.get_key_for_file(mapping, "student_v2") == "student"
    assert ctrl.get_key_for_file(mapping, "student_v2.csv") == "student"

def test_regex_fullmatch_ignorecase(ctrl):
    mapping = {"course": r"^course(?:[._-].+)?\.csv$"}
    assert ctrl.get_key_for_file(mapping, "Course_20240101.CSV") == "course"
    assert ctrl.get_key_for_file(mapping, "COURSE.csv") == "course"
    # ensure fullmatch (not substring)
    assert ctrl.get_key_for_file(mapping, "my_course_20240101.csv") is None

def test_list_values_mixed_literal_and_regex(ctrl):
    mapping = {"student": ["students.csv", r"^stud\d+\.csv$"]}
    assert ctrl.get_key_for_file(mapping, "STUD123.csv") == "student"
    assert ctrl.get_key_for_file(mapping, "students_2024.csv") == "student"

def test_invalid_regex_is_ignored(ctrl):
    mapping = {"bad": ["(unclosed", "ok.csv"]}
    # bad regex should be skipped; literal should match
    assert ctrl.get_key_for_file(mapping, "OK.csv") == "bad"

def test_returns_none_when_no_match(ctrl):
    mapping = {"student": "student.csv"}
    assert ctrl.get_key_for_file(mapping, "unknown.csv") is None