# Todo Service

A small todo API built with FastAPI, persisted to SQLite through SQLAlchemy.
One table — `todos(id, title, done, created_at)` — behind five endpoints:

| Method | Path                | Purpose                                  |
| ------ | ------------------- | ---------------------------------------- |
| POST   | `/todos`            | Create a todo (201)                      |
| GET    | `/todos?done=`      | List todos, optionally filtered by `done` |
| GET    | `/todos/{id}`       | Fetch one todo (404 when missing)        |
| PATCH  | `/todos/{id}`       | Update `title` and/or `done` (404)       |
| DELETE | `/todos/{id}`       | Delete a todo (204, or 404 when missing) |

## Layout

- `app/models.py` — the `todos` table.
- `app/schemas.py` — request/response bodies.
- `app/main.py` — routes plus the `create_app(database_url)` factory.
- `tests/` — pytest suite; every test runs against a fresh temporary database.

## Setup

From this directory (`services/todo/`):

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the server

```sh
uvicorn app.main:app --reload
```

The API is then at http://localhost:8000 (interactive docs at `/docs`).
The database URL defaults to `sqlite:///./todos.db`; set `TODO_DATABASE_URL`
to use another location:

```sh
TODO_DATABASE_URL=sqlite:////tmp/todos.db uvicorn app.main:app
```

## Run the tests

```sh
python -m pytest -v
```

Each test gets its own temporary SQLite file, so the suite never touches the
development `todos.db`. The same command runs in CI on every pull request
(see `.github/workflows/todo-tests.yml`).
