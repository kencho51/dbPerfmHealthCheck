"""Quick upload test script."""
import urllib.request
import json

csv_file = r"C:\Users\kenlcho\Desktop\obsidian\kencho-vault\hkjc\dbPerfmHealthCheck\data\Jan2026\blockersSatJan26.csv"

import http.client
import mimetypes
import os

boundary = "----FormBoundary7MA4YWxkTrZu0gW"

with open(csv_file, "rb") as f:
    file_content = f.read()

filename = os.path.basename(csv_file)
body = (
    f"--{boundary}\r\n"
    f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
    f"Content-Type: text/csv\r\n\r\n"
).encode() + file_content + f"\r\n--{boundary}--\r\n".encode()

req = urllib.request.Request(
    "http://localhost:8000/api/upload",
    data=body,
    method="POST",
    headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
)

try:
    with urllib.request.urlopen(req, timeout=120) as r:
        result = json.loads(r.read())
    print("SUCCESS:")
    print(json.dumps(result, indent=2))
except urllib.error.HTTPError as e:
    print(f"HTTPError {e.code}:")
    print(e.read().decode()[:2000])
except Exception as e:
    print(f"Error: {e}")
