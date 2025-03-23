"""File validation functions for various schemas. (Record by record validation happens in the
pipelines, this is for general file validation.)
"""

import csv

from collections import Counter
from typing import Final

from .utilities import SchemaType


# The standard PDP ARF file columns
PDP_COHORT_COLS: Final = [
    "Institution ID",
    "Cohort",
    "Student GUID",
    "Cohort Term",
    "Student Age",
    "Enrollment Type",
    "Enrollment Intensity First Term",
    "Math Placement",
    "English Placement",
    "Dual and Summer Enrollment",
    "Race",
    "Ethnicity",
    "Gender",
    "First Gen",
    "Pell Status First Year",
    "Attendance Status Term 1",
    "Credential Type Sought Year 1",
    "Program of Study Term 1",
    "GPA Group Term 1",
    "GPA Group Year 1",
    "Number of Credits Attempted Year 1",
    "Number of Credits Earned Year 1",
    "Number of Credits Attempted Year 2",
    "Number of Credits Earned Year 2",
    "Number of Credits Attempted Year 3",
    "Number of Credits Earned Year 3",
    "Number of Credits Attempted Year 4",
    "Number of Credits Earned Year 4",
    "Gateway Math Status",
    "Gateway English Status",
    "AttemptedGatewayMathYear1",
    "AttemptedGatewayEnglishYear1",
    "CompletedGatewayMathYear1",
    "CompletedGatewayEnglishYear1",
    "GatewayMathGradeY1",
    "GatewayEnglishGradeY1",
    "AttemptedDevMathY1",
    "AttemptedDevEnglishY1",
    "CompletedDevMathY1",
    "CompletedDevEnglishY1",
    "Retention",
    "Persistence",
    "Years to Bachelors at cohort inst.",
    "Years to Associates or Certificate at cohort inst.",
    "Years to Bachelor at other inst.",
    "Years to Associates or Certificate at other inst.",
    "Years of Last Enrollment at cohort institution",
    "Years of Last Enrollment at other institution",
    "Time to Credential",
    "Reading Placement",
    "Special Program",
    "NASPA First-Generation",
    "Incarcerated Status",
    "Military Status",
    "Employment Status",
    "Disability Status",
    "Foreign Language Completion",
    "First Year to Bachelors at cohort inst.",
    "First Year to Associates or Certificate at cohort inst.",
    "First Year to Bachelor at other inst.",
    "First Year to Associates or Certificate at other inst.",
    "Program of Study Year 1",
    "Most Recent Bachelors at Other Institution STATE",
    "Most Recent Associates or Certificate at Other Institution STATE",
    "Most Recent Last Enrollment at Other institution STATE",
    "First Bachelors at Other Institution STATE",
    "First Associates or Certificate at Other Institution STATE",
    "Most Recent Bachelors at Other Institution CARNEGIE",
    "Most Recent Associates or Certificate at Other Institution CARNEGIE",
    "Most Recent Last Enrollment at Other institution CARNEGIE",
    "First Bachelors at Other Institution CARNEGIE",
    "First Associates or Certificate at Other Institution CARNEGIE",
    "Most Recent Bachelors at Other Institution LOCALE",
    "Most Recent Associates or Certificate at Other Institution LOCALE",
    "Most Recent Last Enrollment at Other institution LOCALE",
    "First Bachelors at Other Institution LOCALE",
    "First Associates or Certificate at Other Institution LOCALE",
]
PDP_COURSE_COLS: Final = [
    "Student GUID",
    "Student Age",
    "Race",
    "Ethnicity",
    "Gender",
    "Institution ID",
    "Cohort",
    "Cohort Term",
    "Academic Year",
    "Academic Term",
    "Course Prefix",
    "Course Number",
    "Section ID",
    "Course Name",
    "Course CIP",
    "Course Type",
    "Math or English Gateway",
    "Co-requisite Course",
    "Course Begin Date",
    "Course End Date",
    "Grade",
    "Number of Credits Attempted",
    "Number of Credits Earned",
    "Delivery Method",
    "Core Course",
    "Core Course Type",
    "Core Competency Completed",
    "Enrolled at Other Institution(s)",
    "Credential Engine Identifier",
    "Course Instructor Employment Status",
    "Course Instructor Rank",
    "Enrollment Record at Other Institution(s) STATE(s)",
    "Enrollment Record at Other Institution(s) CARNEGIE(s)",
    "Enrollment Record at Other Institution(s) LOCALE(s)",
]

# The PDP aligned SST columns
SST_PDP_COHORT_COLS: Final = [
    "Institution ID",
    "Cohort",
    "Student GUID",
    "Cohort Term",
    "Student Age",
    "Enrollment Type",
    "Enrollment Intensity First Term",
    "Math Placement",
    "English Placement",
    "Dual and Summer Enrollment",
    "Race",
    "Ethnicity",
    "Gender",
    "First Gen",
    "Pell Status First Year",
    "Credential Type Sought Year 1",
    "Program of Study Term 1",
    "GPA Group Term 1",
    "GPA Group Year 1",
    "Retention",
    "Persistence",
    "Years to Bachelors at cohort inst.",
    "Years to Associates or Certificate at cohort inst.",
    "Years to Bachelor at other inst.",
    "Years to Associates or Certificate at other inst.",
    "Years of Last Enrollment at cohort institution",
    "Years of Last Enrollment at other institution",
    "Reading Placement",
    "Special Program",
    "NASPA First-Generation",
    "Military Status",
    "Employment Status",
    "Disability Status",
    "Foreign Language Completion",
    "First Year to Bachelors at cohort inst.",
    "First Year to Associates or Certificate at cohort inst.",
    "First Year to Bachelor at other inst.",
    "First Year to Associates or Certificate at other inst.",
    "Program of Study Year 1",
    "Most Recent Last Enrollment at Other institution STATE",
    "Most Recent Last Enrollment at Other institution CARNEGIE",
    "Most Recent Last Enrollment at Other institution LOCALE",
]
SST_PDP_COURSE_COLS: Final = [
    "Student GUID",
    "Student Age",
    "Race",
    "Ethnicity",
    "Gender",
    "Institution ID",
    "Academic Year",
    "Academic Term",
    "Course Prefix",
    "Course Number",
    "Section ID",
    "Course CIP",
    "Course Type",
    "Math or English Gateway",
    "Co-requisite Course",
    "Course Begin Date",
    "Course End Date",
    "Grade",
    "Number of Credits Attempted",
    "Number of Credits Earned",
    "Delivery Method",
    "Core Course",
    "Core Course Type",
    "Core Competency Completed",
    "Enrolled at Other Institution(s)",
    "Credential Engine Identifier",
    "Course Instructor Rank",
]
SST_PDP_FINANCE_COLS: Final = [
    "Student ID",
    "Institution ID",
    "Academic Year",
    "Dependency Status",
    "Housing Status",
    "Cost of Attendance",
    "EFC",
    "Total Institutional Grants",
    "Total State Grants",
    "Total Federal Grants",
    "Unmet Need",
    "Net Price",
    "Applied Aid",
]

# Optional Fields
PDP_COHORT_OPTIONAL_COLS: Final = [
    "Reading Placement",
    "Special Program",
    "NASPA First-Generation",
    "Incarcerated Status",
    "Military Status",
    "Employment Status",
    "Disability Status",
    "Foreign Language Completion",
]
PDP_COURSE_OPTIONAL_COLS: Final = [
    "Credential Engine Identifier",
    "Course Instructor Employment Status",
    "Course Instructor Rank",
]

SCHEMA_TYPE_TO_COLS: Final = {
    SchemaType.PDP_COHORT: PDP_COHORT_COLS,
    SchemaType.PDP_COURSE: PDP_COURSE_COLS,
    SchemaType.SST_PDP_COHORT: SST_PDP_COHORT_COLS,
    SchemaType.SST_PDP_COURSE: SST_PDP_COURSE_COLS,
    SchemaType.SST_PDP_FINANCE: SST_PDP_FINANCE_COLS,
}

SCHEMA_TYPE_TO_OPTIONAL_COLS: Final = {
    SchemaType.PDP_COHORT: PDP_COHORT_OPTIONAL_COLS,
    SchemaType.PDP_COURSE: PDP_COURSE_OPTIONAL_COLS,
    SchemaType.SST_PDP_COHORT: PDP_COHORT_OPTIONAL_COLS,
    SchemaType.SST_PDP_COURSE: PDP_COURSE_OPTIONAL_COLS,
    # The financial file does not have optional fields.
    SchemaType.SST_PDP_FINANCE: [],
}


def valid_subset_lists(
    superset_list: list[str], subset_list: list[str], optional_list: list[str]
) -> bool:
    """Checks if the subset_list is a subset of or equivalent to superset_list. And if so,
    whether the missing values are all present in the optional list. This method disregards order
    but cares about duplicates."""
    # Checks if any value in subset list is NOT present in superset list.
    if Counter(subset_list) - Counter(superset_list):
        # This is not a valid state, users should not be passing in unrecognized columns.
        return False
    missing_vals = Counter(superset_list) - Counter(subset_list)
    return not Counter(missing_vals) - Counter(optional_list)


def detect_file_type(col_names: list[str]) -> set[SchemaType]:
    """Returns all schemas that match for a list of col names."""
    res = set()
    for schema, schema_cols in SCHEMA_TYPE_TO_COLS.items():
        optional_cols = SCHEMA_TYPE_TO_OPTIONAL_COLS[schema]
        if valid_subset_lists(schema_cols, col_names, optional_cols):
            res.add(schema)
    if not res:
        # If it doesn't match any, it will match unknown.
        res.add(SchemaType.UNKNOWN)
    return res


def validate_file(filename: str, allowed_types: set[SchemaType]) -> set[SchemaType]:
    """Validates given a filename."""
    with open(filename) as f:
        return validate_file_reader(f, allowed_types)


def get_col_names(f) -> None:
    """Get column names."""
    try:
        # Use the sniffer to detect the columns and dialect.
        csv_dialect = csv.Sniffer().sniff(f.readline())
        f.seek(0)
        if not csv.Sniffer().has_header(f.readline()):
            raise ValueError("CSV file malformed: Headers not found")
    except csv.Error as e:
        raise ValueError(f"CSV file malformed: {e}") from e
    # Read the column names and store in col_names.
    f.seek(0)
    dict_reader = csv.DictReader(f, dialect=csv_dialect)
    col_names = dict_reader.fieldnames
    return col_names


def validate_file_reader(reader, allowed_types: set[SchemaType]) -> set[SchemaType]:
    """Validates given a reader. Returns only if a valid format was found, otherwise raises error"""
    if not allowed_types:
        raise ValueError("CSV file schema not recognized")
    res = detect_file_type(get_col_names(reader))
    if any(i in allowed_types for i in res):
        return res
    raise ValueError("CSV file schema not recognized")
