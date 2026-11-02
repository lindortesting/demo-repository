"""Todo service: a small FastAPI API backed by SQLite through SQLAlchemy.

- `models` holds the single `todos` table.
- `schemas` holds the request/response bodies.
- `main` holds the routes and exposes `create_app(database_url)` plus the
  module-level `app` that uvicorn serves.
"""
