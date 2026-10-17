import hashlib
import os
import re


def compute_sha256(content: bytes) -> str:
    """Calculate SHA-256 hex digest for given bytes content."""
    hasher = hashlib.sha256()
    hasher.update(content)
    return hasher.hexdigest()


def sanitize_filename(filename: str) -> str:
    """
    Sanitize user filename to prevent path traversal attacks.
    Removes path components (../../) and unsafe characters.
    """
    if not filename:
        return "unnamed_file"
    
    # Extract basename only to prevent directory traversal
    name = os.path.basename(filename)
    
    # Replace any control characters or invalid path characters with underscores
    name = re.sub(r'[\\/:\*\?"<>\|]', '_', name)
    
    # Trim leading/trailing spaces or dots
    name = name.strip(". ")
    
    return name if name else "unnamed_file"
