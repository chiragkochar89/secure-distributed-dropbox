import os
import re

PATTERNS = [
    re.compile(r'(JWT_SECRET_KEY|POSTGRES_PASSWORD|MINIO_SECRET_KEY|password_hash)\s*[:=]\s*[\'"]([^\'"]+)[\'"]', re.IGNORECASE),
    re.compile(r'(eyJ[a-zA-Z0-9_-]{10,}\.eyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,})')  # JWT pattern
]

IGNORE_DIRS = {'.venv', 'node_modules', 'data', '.git', '__pycache__', '.pytest_cache'}
EXTENSIONS = ('.py', '.yml', '.yaml', '.json', '.md', '.ts', '.tsx', '.env', '.example')

findings = []

for root, dirs, files in os.walk('.'):
    dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
    for f in files:
        if f.endswith(EXTENSIONS):
            path = os.path.join(root, f)
            with open(path, 'r', encoding='utf-8', errors='ignore') as fh:
                for line_no, line in enumerate(fh, 1):
                    for p in PATTERNS:
                        m = p.search(line)
                        if m:
                            findings.append((path, line_no, line.strip()))

print(f"Total secret pattern matches: {len(findings)}")
for path, line_no, line in findings:
    print(f"  {path}:{line_no} -> {line[:80]}...")
