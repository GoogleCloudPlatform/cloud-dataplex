# Utility convenience functions
import argparse
import sys
from datetime import datetime

# Returns string content from file at given path
def loadReferencedFile(file_path) -> str:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content
    except Exception as e:
        print(f"Error while reading file {file_path}: {e}")
        print("Exiting")
        sys.exit(1)
    return None

# folder name with timestamp
def generateFolderName(SOURCE_TYPE : str):
    currentDate = datetime.now()
    return f"{SOURCE_TYPE}/{currentDate.year}{currentDate.month}{currentDate.day}-{currentDate.hour}{currentDate.minute}{currentDate.second}"
      