"""
Root conftest.py — makes 'app', 'config', etc. importable from any test file
regardless of the working directory pytest is invoked from.

Place this file at the project root (next to config.py and runserver.py).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))