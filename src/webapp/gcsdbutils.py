"""
Helper functions that use storage and session.
"""

from sqlalchemy import and_, update
from sqlalchemy.future import select

from .utilities import (
    str_to_uuid,
    get_external_bucket_name,
    SchemaType,
)

from .database import (
    FileTable,
    JobTable,
)


def get_job_id(filename: str) -> int:
    """From a fully qualified file name (i.e. everything sub-bucket name level), get the job id."""
    tmp = get_filename_without_approve_dir(filename)
    return int(tmp.split("/")[0])


def get_filename_without_approve_dir(filename: str) -> int:
    """Remove the approved or unapproved prefix as that isn't a property of the filename itself
    and may change if the file gets approved or unapproved."""
    if filename.startswith("approved/"):
        return filename.removeprefix("approved/")
    if filename.startswith("unapproved/"):
        return filename.removeprefix("unapproved/")
    return filename


def is_file_approved(filename: str) -> bool:
    """Checks if a file is approved, which is based on its bucket in GCP.
    This should of course be called before any prefix stripping."""
    if filename.startswith("approved/"):
        return True
    if filename.startswith("unapproved/"):
        return False
    raise ValueError("Unexpected filename structure.")


def update_db_from_bucket(inst_id: str, session, storage_control):
    """Updates the sql tables by checking if there are new files in the bucket.

    Note that while all output files will be added to the file table, potentially with their own approval status, the JobTable will only refer to the csv inference output and indicate validity (approval) value for that file.
    This means that for a single run it's possible to have some output files be approved and some be unapproved but that is confusing and we discourage it.
    Note that deleted files are handled upon file retrieval, not here."""
    dir_prefix = ["approved/", "unapproved/"]
    all_files = []
    for d in dir_prefix:
        all_files = all_files + storage_control.list_blobs_in_folder(
            get_external_bucket_name(inst_id), d
        )
    new_files_to_add_to_database = []
    for f in all_files:
        # We only handle png and csv outputs.
        if not f.endswith(".png") and not f.endswith(".csv"):
            continue
        file_approved = is_file_approved(f)
        # We strip the approved/unapproved prefix since the file can move between the two and should still be considered one file.
        f = get_filename_without_approve_dir(f)
        # Check if that file already exists in the table, otherwise add it.
        query_result = session.execute(
            select(FileTable).where(
                and_(
                    FileTable.name == f,
                    FileTable.inst_id == str_to_uuid(inst_id),
                )
            )
        ).all()
        if len(query_result) == 0:
            new_files_to_add_to_database.append(
                FileTable(
                    name=f,
                    inst_id=str_to_uuid(inst_id),
                    sst_generated=True,
                    schemas=[
                        SchemaType.PNG if f.endswith(".png") else SchemaType.SST_OUTPUT
                    ],
                    valid=file_approved,
                )
            )
            if f.endswith(".csv"):
                query = (
                    update(JobTable)
                    .where(JobTable.id == get_job_id(f))
                    .values(
                        output_filename=f, completed=True, output_valid=file_approved
                    )
                )
                session.execute(query)
        elif len(query_result) == 1:
            # This file already exists, check if its status has changed.
            if query_result[0][0].valid != file_approved:
                # Technically you could make an approved file unapproved.
                query = (
                    update(FileTable)
                    .where(
                        and_(
                            FileTable.name == f,
                            FileTable.inst_id == str_to_uuid(inst_id),
                        )
                    )
                    .values(valid=file_approved)
                )
                session.execute(query)
        else:
            raise ValueError("Attempted creation of file with duplicate name.")
    for elem in new_files_to_add_to_database:
        session.add(elem)
