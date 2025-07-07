import os
import shutil

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