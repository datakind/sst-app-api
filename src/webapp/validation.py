"""File validation functions for various schemas. (Record by record validation happens in the
pipelines, this is for general file validation.)
"""

import csv

from collections import Counter
from typing import Final, Any, NamedTuple, List

from .utilities import SchemaType

# The PDP aligned SST columns
SST_PDP_COHORT_COLS: Final = [
    'institution_id',
    'cohort',
    'student_guid',
    'cohort_term',
    'student_age',
    'enrollment_type',
    'enrollment_intensity_first_term',
    'math_placement',
    'english_placement',
    'dual_and_summer_enrollment',
    'race',
    'ethnicity',
    'gender',
    'first_gen',
    'pell_status_first_year',
    'credential_type_sought_year_1',
    'program_of_study_term_1',
    'gpa_group_term_1',
    'gpa_group_year_1',
    'retention',
    'persistence',
    'years_to_bachelors_at_cohort_inst.',
    'years_to_associates_or_certificate_at_cohort_inst.',
    'years_to_bachelor_at_other_inst.',
    'years_to_associates_or_certificate_at_other_inst.',
    'years_of_last_enrollment_at_cohort_institution',
    'years_of_last_enrollment_at_other_institution',
    'reading_placement',
    'special_program',
    'naspa_first-generation',
    'military_status',
    'employment_status',
    'disability_status',
    'foreign_language_completion',
    'first_year_to_bachelors_at_cohort_inst.',
    'first_year_to_associates_or_certificate_at_cohort_inst.',
    'first_year_to_bachelor_at_other_inst.',
    'first_year_to_associates_or_certificate_at_other_inst.',
    'program_of_study_year_1',
    'most_recent_last_enrollment_at_other_institution_state',
    'most_recent_last_enrollment_at_other_institution_carnegie',
    'most_recent_last_enrollment_at_other_institution_locale'
    ]

SST_PDP_COURSE_COLS: Final = [
    'student_guid',
    'student_age',
    'race',
    'ethnicity',
    'gender',
    'institution_id',
    'academic_year',
    'academic_term',
    'course_prefix',
    'course_number',
    'section_id',
    'course_cip',
    'course_type',
    'math_or_english_gateway',
    'co-requisite_course',
    'course_begin_date',
    'course_end_date',
    'grade',
    'number_of_credits_attempted',
    'number_of_credits_earned',
    'delivery_method',
    'core_course',
    'core_course_type',
    'core_competency_completed',
    'enrolled_at_other_institution(s)',
    'credential_engine_identifier',
    'course_instructor_rank'
    ]

# Required finance fields (e.g., Pell and related key fields)
SST_PDP_FINANCE_COLS: Final = [
    'student_id',
    'pell_status_first_year'
]

# Optional Fields
PDP_COHORT_OPTIONAL_COLS: Final = [
    'reading_placement',
    'special_program',
    'naspa_first-generation',
    'incarcerated_status',
    'military_status',
    'employment_status',
    'disability_status',
    'foreign_language_completion',
    'years_to_latest_associates_at_cohort_inst',
    'years_to_latest_certificate_at_cohort_inst',
    'years_to_latest_associates_at_other_inst',
    'years_to_latest_certificate_at_other_inst',
    'first_year_to_associates_at_cohort_inst',
    'first_year_to_certificate_at_cohort_inst',
    'first_year_to_associates_at_other_inst',
    'first_year_to_certificate_at_other_inst'
    ]

PDP_COURSE_OPTIONAL_COLS: Final = [
    'credential_engine_identifier',
    'course_instructor_employment_status',
    'course_instructor_rank'
    ]


# Optional finance fields (formerly all finance fields)
PDP_FINANCE_OPTIONAL_COLS: Final = [
    'dependency_status',
    'housing_status',
    'cost_of_attendance',
    'efc',
    'total_institutional_grants',
    'total_state_grants',
    'total_federal_grants',
    'unmet_need',
    'net_price',
    'applied_aid'
]

SCHEMA_TYPE_TO_COLS: Final = {
    SchemaType.SST_PDP_COHORT: SST_PDP_COHORT_COLS,
    SchemaType.SST_PDP_COURSE: SST_PDP_COURSE_COLS,
    SchemaType.SST_PDP_FINANCE: SST_PDP_FINANCE_COLS,
}

SCHEMA_TYPE_TO_OPTIONAL_COLS: Final = {
    SchemaType.SST_PDP_COHORT: PDP_COHORT_OPTIONAL_COLS,
    SchemaType.SST_PDP_COURSE: PDP_COURSE_OPTIONAL_COLS,
    SchemaType.SST_PDP_FINANCE: PDP_FINANCE_OPTIONAL_COLS,
}


class ColumnValidationResult(NamedTuple):
    is_valid: bool
    unexpected_columns: list[str]
    missing_required_columns: list[str]


def validate_file(filename: str, allowed_types: set[SchemaType]) -> set[SchemaType]:
    """Validates given a filename."""
    with open(filename) as f:
        return validate_file_reader(f, allowed_types)


def validate_file_reader(
    reader: Any, allowed_types: set[SchemaType]
) -> set[SchemaType]:
    """Validates given a reader. Returns only if a valid format was found, otherwise raises error"""
    if not allowed_types:
        raise ValueError("CSV file schema not recognized")

    file_columns = get_col_names(reader)

    required_schemas = allowed_types - {SchemaType.SST_PDP_FINANCE}
    return detect_file_type(file_columns, required_schemas)


def get_col_names(f: Any) -> list[str]:
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
    col_names = [col.replace(" ", "_").lower() for col in col_names]
    return col_names


def detect_file_type(col_names: list[str], required_schemas: set[SchemaType]) -> set[SchemaType]:
    """Returns all schemas that match for a list of col names. Raises error if required schemas aren't matched."""
    matches = set()
    errors = {}

    for schema, expected_cols in SCHEMA_TYPE_TO_COLS.items():
        optional_cols = SCHEMA_TYPE_TO_OPTIONAL_COLS[schema]
        result = valid_subset_lists(expected_cols, col_names, optional_cols)

        if result.is_valid:
            matches.add(schema)
        else:
            errors[schema.name] = result

    if matches & required_schemas:
        return matches

    error_msgs = []
    for schema_name, res in errors.items():
        msg = f"\nSchema: {schema_name}"
        if res.unexpected_columns:
            msg += f"\n Unexpected columns: {res.unexpected_columns}"
        if res.missing_required_columns:
            msg += f"\n Missing required columns: {res.missing_required_columns}"
        error_msgs.append(msg)

    raise ValueError(
        "Required file schema(s) not recognized. Details of mismatches:\n" + "\n".join(error_msgs)
    )

def valid_subset_lists(
    expected: list[str], actual: list[str], optional_list: list[str]
) -> ColumnValidationResult:
    """Validates expected vs actual columns with optional overrides."""
    expected_counter = Counter(expected)
    actual_counter = Counter(actual)

    unexpected = list((actual_counter - expected_counter).elements())

    missing_total = list((expected_counter - actual_counter).elements())
    missing_required = [col for col in missing_total if col not in optional_list]

    is_valid = not unexpected and not missing_required

    return ColumnValidationResult(
        is_valid=is_valid,
        unexpected_columns=unexpected,
        missing_required_columns=missing_required,
    )