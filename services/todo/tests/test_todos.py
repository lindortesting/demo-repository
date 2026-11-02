"""Tests for the todo API. Each test encodes one requirement and says why it exists."""
from datetime import datetime, timedelta, timezone


def create(client, title="buy milk"):
    response = client.post("/todos", json={"title": title})
    assert response.status_code == 201
    return response.json()


class TestCreate:
    def test_create_returns_201_with_populated_body(self, client):
        """Requirement: POST /todos answers 201 with id, title, done=False, created_at."""
        response = client.post("/todos", json={"title": "buy milk"})
        assert response.status_code == 201
        body = response.json()
        assert body["id"] >= 1
        assert body["title"] == "buy milk"
        assert body["done"] is False
        datetime.fromisoformat(body["created_at"])  # must be a valid timestamp

    def test_create_persists_the_todo(self, client):
        """Requirement: a created todo can be read back — writes reach the database."""
        created = create(client)
        assert client.get(f"/todos/{created['id']}").json() == created

    def test_ids_are_unique_per_todo(self, client):
        """Requirement: every todo gets its own id from the autoincrement key."""
        a, b = create(client, "a"), create(client, "b")
        assert a["id"] != b["id"]

    def test_create_rejects_missing_title(self, client):
        """Requirement: title is required — a body without it is a 422, not a 500."""
        assert client.post("/todos", json={}).status_code == 422

    def test_create_rejects_empty_title(self, client):
        """Requirement: titles must be non-empty (min_length=1)."""
        assert client.post("/todos", json={"title": ""}).status_code == 422

    def test_create_rejects_overlong_title(self, client):
        """Requirement: titles are capped at 200 characters to bound row size."""
        assert client.post("/todos", json={"title": "x" * 201}).status_code == 422

    def test_created_at_is_stamped_by_the_server(self, client):
        """Requirement: created_at is set at creation time, in UTC, by the server."""
        before = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=5)
        stamp = datetime.fromisoformat(create(client)["created_at"])
        after = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=5)
        assert before <= stamp <= after


class TestList:
    def test_list_empty_database_returns_empty_list(self, client):
        """Requirement: GET /todos on an empty database is 200 [], not an error."""
        response = client.get("/todos")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_returns_all_todos_in_creation_order(self, client):
        """Requirement: GET /todos returns every todo ordered by id."""
        todos = [create(client, f"todo {i}") for i in range(3)]
        listed = client.get("/todos").json()
        assert [t["id"] for t in listed] == [t["id"] for t in todos]

    def test_list_filter_done_true(self, client):
        """Requirement: ?done=true returns only completed todos."""
        create(client, "open")
        closed = create(client, "closed")
        client.patch(f"/todos/{closed['id']}", json={"done": True})
        listed = client.get("/todos", params={"done": "true"}).json()
        assert [t["id"] for t in listed] == [closed["id"]]

    def test_list_filter_done_false(self, client):
        """Requirement: ?done=false returns only incomplete todos."""
        opened = create(client, "open")
        closed = create(client, "closed")
        client.patch(f"/todos/{closed['id']}", json={"done": True})
        listed = client.get("/todos", params={"done": "false"}).json()
        assert [t["id"] for t in listed] == [opened["id"]]

    def test_list_rejects_invalid_done_value(self, client):
        """Requirement: ?done= must parse as a boolean; garbage is a 422."""
        assert client.get("/todos", params={"done": "maybe"}).status_code == 422


class TestGet:
    def test_get_returns_the_todo(self, client):
        """Requirement: GET /todos/{id} returns the stored todo."""
        created = create(client)
        response = client.get(f"/todos/{created['id']}")
        assert response.status_code == 200
        assert response.json() == created

    def test_get_missing_id_returns_404(self, client):
        """Requirement: a missing id is a 404 — never a 500 or a null result."""
        response = client.get("/todos/999")
        assert response.status_code == 404
        assert response.json() == {"detail": "Todo not found"}


class TestUpdate:
    def test_update_title_only(self, client):
        """Requirement: PATCH may change title while leaving done untouched."""
        created = create(client)
        response = client.patch(f"/todos/{created['id']}", json={"title": "buy oat milk"})
        assert response.status_code == 200
        assert response.json()["title"] == "buy oat milk"
        assert response.json()["done"] is False

    def test_update_done_only(self, client):
        """Requirement: PATCH may change done while leaving title untouched."""
        created = create(client, "keep me")
        response = client.patch(f"/todos/{created['id']}", json={"done": True})
        assert response.json()["done"] is True
        assert response.json()["title"] == "keep me"

    def test_update_explicit_done_false(self, client):
        """Regression: done=false must apply — it is not the same as omitting done."""
        created = create(client)
        client.patch(f"/todos/{created['id']}", json={"done": True})
        response = client.patch(f"/todos/{created['id']}", json={"done": False})
        assert response.json()["done"] is False

    def test_update_empty_body_is_a_noop(self, client):
        """Requirement: PATCH with no fields changes nothing and still returns 200."""
        created = create(client)
        response = client.patch(f"/todos/{created['id']}", json={})
        assert response.status_code == 200
        assert response.json() == created

    def test_update_missing_id_returns_404(self, client):
        """Requirement: PATCH on a missing id is a 404."""
        assert client.patch("/todos/999", json={"done": True}).status_code == 404

    def test_update_rejects_empty_title(self, client):
        """Requirement: the title length rules apply to updates too."""
        created = create(client)
        assert client.patch(f"/todos/{created['id']}", json={"title": ""}).status_code == 422


class TestDelete:
    def test_delete_returns_204_and_removes_the_todo(self, client):
        """Requirement: DELETE removes the row; reads afterwards return 404."""
        created = create(client)
        assert client.delete(f"/todos/{created['id']}").status_code == 204
        assert client.get(f"/todos/{created['id']}").status_code == 404
        assert client.get("/todos").json() == []

    def test_delete_missing_id_returns_404(self, client):
        """Requirement: DELETE on a missing id is a 404."""
        assert client.delete("/todos/999").status_code == 404
