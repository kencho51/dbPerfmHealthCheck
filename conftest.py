"""Root conftest — ensure the project root is on sys.path so `api.*` imports work."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
