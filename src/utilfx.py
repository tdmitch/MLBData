import os
import shutil
import zipfile
import glob

def move_files(source_dir, destination_dir, extension=None):
    """
    Move downloaded files from the source directory to the destination directory.
    If 'extension' is specified (e.g., '.json'), only move files with that extension (case-insensitive).
    If 'extension' is None, move all files.
    """
    for filename in os.listdir(source_dir):
        src = os.path.join(source_dir, filename)
        dst = os.path.join(destination_dir, filename)
        if os.path.isfile(src):
            if extension is None or filename.lower().endswith(extension.lower()):
                shutil.move(src, dst)


def archive_files(directory, file_pattern, archive_name_pattern):
    """
    Archive files in the specified directory matching file_pattern into a .zip archive.
    The archive will be named using archive_name_pattern (e.g., 'archive_{date}.zip').
    After archiving, the original files are deleted.

    Args:
        directory (str): Path to the directory containing files.
        file_pattern (str): Pattern to match files (supports wildcards, e.g., '*.json').
        archive_name_pattern (str): Pattern for the archive filename (e.g., 'archive.zip').
    """
    # Find matching files
    search_pattern = os.path.join(directory, file_pattern)
    files_to_archive = glob.glob(search_pattern)
    if not files_to_archive:
        return  # Nothing to archive

    archive_path = os.path.join(directory, archive_name_pattern)
    with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as archive:
        for file_path in files_to_archive:
            arcname = os.path.basename(file_path)
            archive.write(file_path, arcname)
    # Delete original files
    for file_path in files_to_archive:
        os.remove(file_path)