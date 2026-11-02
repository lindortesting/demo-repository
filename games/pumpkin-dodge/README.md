# Pumpkin Dodge

A zero-dependency browser game: move a basket with the arrow keys, catch the
falling pumpkins, dodge the falling rocks. Plain HTML, CSS and JavaScript —
no build step, no libraries.

## Play

ES modules do not load over `file://`, so serve the repo over HTTP:

```sh
python3 -m http.server 8000   # from the repo root
```

Then open http://localhost:8000/games/pumpkin-dodge/.

## Rules

- Move the basket with the `←` and `→` arrow keys.
- Catch a pumpkin: **+10 points**.
- Catch a rock: **lose a life**.
- Let a pumpkin fall off the screen: **lose a life**.
- Dodge a rock (it falls off the screen): nothing happens — good dodge.
- You start with **3 lives**; at zero the game is over and **Play again** restarts.
- The game speeds up over time: objects spawn more often and fall faster.

## Layout

- `logic.js` — every game rule (spawning, movement, collision, scoring,
  lives, difficulty) as pure functions with no DOM access. Callers pass the
  time step and a random source, so a game is fully deterministic.
- `game.js` — canvas renderer, keyboard input and HUD; imports `logic.js`.
- `index.html`, `styles.css` — the page.
- `test/logic.test.js` — the rules as unit tests.

## Test

```sh
cd games/pumpkin-dodge
node --test
```
