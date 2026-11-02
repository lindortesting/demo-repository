"""FastAPI app for the todo service.

`create_app(database_url)` builds a self-contained app against any SQLite
database — the tests use it with a temporary file. The module-level `app`
is what uvicorn serves; it is configured by the TODO_DATABASE_URL
environment variable and defaults to `sqlite:///./todos.db`.
"""
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from .models import Base, Todo
from .schemas import TodoCreate, TodoOut, TodoUpdate


def create_app(database_url: str) -> FastAPI:
    """Build the todo API against `database_url`; tables are created at startup."""
    engine = create_engine(database_url, connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        Base.metadata.create_all(engine)
        yield

    app = FastAPI(title="Todo Service", lifespan=lifespan)

    def get_session():
        with SessionLocal() as session:
            yield session

    def get_todo_or_404(todo_id: int, session: Session) -> Todo:
        todo = session.get(Todo, todo_id)
        if todo is None:
            raise HTTPException(status_code=404, detail="Todo not found")
        return todo

    @app.post("/todos", response_model=TodoOut, status_code=201)
    def create_todo(payload: TodoCreate, session: Session = Depends(get_session)):
        todo = Todo(title=payload.title)
        session.add(todo)
        session.commit()
        return todo

    @app.get("/todos", response_model=list[TodoOut])
    def list_todos(done: bool | None = Query(default=None), session: Session = Depends(get_session)):
        stmt = select(Todo).order_by(Todo.id)
        if done is not None:
            stmt = stmt.where(Todo.done.is_(done))
        return session.scalars(stmt).all()

    @app.get("/todos/{todo_id}", response_model=TodoOut)
    def get_todo(todo_id: int, session: Session = Depends(get_session)):
        return get_todo_or_404(todo_id, session)

    @app.patch("/todos/{todo_id}", response_model=TodoOut)
    def update_todo(todo_id: int, payload: TodoUpdate, session: Session = Depends(get_session)):
        todo = get_todo_or_404(todo_id, session)
        if payload.title is not None:
            todo.title = payload.title
        if payload.done is not None:
            todo.done = payload.done
        session.commit()
        return todo

    @app.delete("/todos/{todo_id}", status_code=204)
    def delete_todo(todo_id: int, session: Session = Depends(get_session)):
        session.delete(get_todo_or_404(todo_id, session))
        session.commit()

    return app


app = create_app(os.environ.get("TODO_DATABASE_URL", "sqlite:///./todos.db"))
