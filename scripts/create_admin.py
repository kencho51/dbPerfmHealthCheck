"""
One-time script: create the first admin user.

Run from the project root:
    uv run python scripts/create_admin.py

If users already exist, this script does nothing (the /register endpoint
is locked after the first user is created).
Uses only Python standard library — no extra dependencies required.
"""
import getpass
import json
import urllib.error
import urllib.request


API = "http://localhost:8000/api"


def main():
    print("=== DB Perfm Analysis — Create Admin User ===\n")
    username = input("Username: ").strip()
    email = input("Email: ").strip()
    password = getpass.getpass("Password (min 8 chars): ")

    if len(password) < 8:
        print("ERROR: Password must be at least 8 characters.")
        return

    payload = json.dumps({"username": username, "email": email, "password": password}).encode()
    req = urllib.request.Request(
        f"{API}/auth/register",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req) as resp:
            user = json.loads(resp.read())
        print(f"\nAdmin user '{user['username']}' created successfully!")
        print(f"  Role  : {user['role']}")
        print(f"  Email : {user['email']}")
        print(f"\nYou can now log in at http://localhost:3000/login")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        if e.code == 403:
            print("\nINFO: A user already exists. Use the admin panel to create more users.")
            print("      Log in at http://localhost:3000/login")
        else:
            print(f"\nERROR {e.code}: {body}")
    except urllib.error.URLError as e:
        print(f"\nERROR: Could not connect to {API}. Make sure the FastAPI server is running.\n{e.reason}")


if __name__ == "__main__":
    main()
