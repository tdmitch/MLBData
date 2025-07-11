import os
import shutil
import zipfile
import glob
import requests
import time

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


def archive_files(directory, file_pattern, archive_filename):
    """
    Archive files in the specified directory matching file_pattern into a .zip archive.
    The archive will be named with the value of the archive_filename argument.
    After archiving, the original files are deleted.

    Args:
        directory (str): Path to the directory containing files.
        file_pattern (str): Pattern to match files (supports wildcards, e.g., '*.json').
        archive_filename (str): Filename for the archive (zip) file.
    """
    # Find matching files
    search_pattern = os.path.join(directory, file_pattern)
    files_to_archive = glob.glob(search_pattern)
    if not files_to_archive:
        return  # Nothing to archive

    archive_path = os.path.join(directory, archive_filename)
    with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as archive:
        for file_path in files_to_archive:
            arcname = os.path.basename(file_path)
            archive.write(file_path, arcname)
    # Delete original files
    for file_path in files_to_archive:
        os.remove(file_path)


def try_get_json(url, retries = 5, pause_minutes = 3):
    """
    Attempts to GET JSON data from the specified URL, retrying on failure.

    Args:
        url (str): The full URL to request.
        retries (int): Number of retry attempts.
        pause_minutes (int): Minutes to pause between retries.

    Returns:
        dict or None: The JSON response if successful, otherwise None.
    """
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Attempt {attempt}: Received status code {response.status_code}. Retrying...")
        except Exception as e:
            print(f"Attempt {attempt}: Error occurred - {e}. Retrying...")
        if attempt < retries:
            time.sleep(pause_minutes * 60)
    print(f"Failed to retrieve data from {url} after {retries} retries.")
    return None