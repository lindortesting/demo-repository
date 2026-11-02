"""Shared fixtures: every test runs against a fresh temporary database."""
import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    """A TestClient backed by a brand-new SQLite file under pytest's tmp_path."""
    with TestClient(create_app(f"sqlite:///{tmp_path}/todos.db")) as test_client:
        yield test_client
