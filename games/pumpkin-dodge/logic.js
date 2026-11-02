/**
 * Pumpkin Dodge — the pure rules of the game.
 *
 * Every rule (spawning, movement, collision, scoring, lives, difficulty
 * ramp) lives here as pure functions over plain state objects. This module
 * never touches the DOM, the clock, or `Math.random`: callers pass in the
 * elapsed time step `dt` and a random source `rng` (a function returning a
 * number in [0, 1)), so a whole game is deterministic and testable under
 * `node --test`. `game.js` renders the state this module produces.
 *
 * Key exports:
 * - `DEFAULT_CONFIG` — every tuning constant, one source of truth.
 * - `createGame(config)` — the initial state of a fresh game.
 * - `step(state, dt, rng, input)` — advance one tick, returning a new state.
 * - `spawnInterval`, `fallSpeed`, `spawnObject`, `overlapsBasket` — the
 *   individual rules `step` is built from, exported for tests and tuning.
 *
 * A state looks like:
 *   { config, status: 'playing' | 'over', score, lives, elapsed,
 *     spawnTimer, basketX, objects: [{ kind: 'pumpkin' | 'rock', x, y, speed }] }
 */

export const DEFAULT_CONFIG = Object.freeze({
  width: 480,
  height: 640,
  basketWidth: 88,
  basketHeight: 26,
  basketSpeed: 340, // px per second
  objectRadius: 15,
  startLives: 3,
  pumpkinPoints: 10,
  pumpkinChance: 0.7, // vs. rock
  spawnStart: 0.9, // seconds between spawns at t=0
  spawnMin: 0.35, // fastest spawn interval
  spawnRamp: 0.008, // interval shrink per elapsed second
  speedStart: 150, // fall px/s at t=0
  speedMax: 340, // fastest fall speed
  speedRamp: 4, // fall px/s gained per elapsed second
});

const NO_INPUT = Object.freeze({ left: false, right: false });

const clamp = (value, lo, hi) => Math.min(hi, Math.max(lo, value));

export function createGame(config = DEFAULT_CONFIG) {
  return {
    config,
    status: 'playing',
    score: 0,
    lives: config.startLives,
    elapsed: 0,
    spawnTimer: config.spawnStart,
    basketX: config.width / 2,
    objects: [],
  };
}

/** Seconds until the next spawn, shrinking with elapsed time down to a floor. */
export function spawnInterval(elapsed, config) {
  return Math.max(config.spawnMin, config.spawnStart - elapsed * config.spawnRamp);
}

/** Fall speed in px/s, growing with elapsed time up to a ceiling. */
export function fallSpeed(elapsed, config) {
  return Math.min(config.speedMax, config.speedStart + elapsed * config.speedRamp);
}

/**
 * Build one falling object just above the top edge. `rng` is consumed
 * exactly twice: once for the kind, once for the horizontal position.
 */
export function spawnObject(elapsed, rng, config) {
  const r = config.objectRadius;
  return {
    kind: rng() < config.pumpkinChance ? 'pumpkin' : 'rock',
    x: r + rng() * (config.width - 2 * r),
    y: -r,
    speed: fallSpeed(elapsed, config),
  };
}

/** True when the circular object touches the basket rectangle. */
export function overlapsBasket(object, basketX, config) {
  const left = basketX - config.basketWidth / 2;
  const top = config.height - config.basketHeight;
  const dx = object.x - clamp(object.x, left, left + config.basketWidth);
  const dy = object.y - clamp(object.y, top, config.height);
  return dx * dx + dy * dy <= config.objectRadius * config.objectRadius;
}

/**
 * Advance the game by `dt` seconds and return the new state; the input
 * state is never mutated. At most one object spawns per tick — frame `dt`
 * values are far below a spawn interval, so the deficit is irrelevant.
 * Once `status` is 'over' the state is final and returned unchanged.
 */
export function step(state, dt, rng, input = NO_INPUT) {
  if (state.status === 'over') return state;
  const { config } = state;
  const elapsed = state.elapsed + dt;
  const direction = (input.right ? 1 : 0) - (input.left ? 1 : 0);
  const basketX = clamp(
    state.basketX + direction * config.basketSpeed * dt,
    config.basketWidth / 2,
    config.width - config.basketWidth / 2,
  );
  const timer = state.spawnTimer - dt;
  const spawnDue = timer <= 0;
  const moved = state.objects.map((object) => ({ ...object, y: object.y + object.speed * dt }));
  return resolve({
    ...state,
    elapsed,
    basketX,
    spawnTimer: spawnDue ? spawnInterval(elapsed, config) : timer,
    objects: spawnDue ? [...moved, spawnObject(elapsed, rng, config)] : moved,
  });
}

/**
 * Score catches and misses, drop resolved objects, and end the game at
 * zero lives. Catching a pumpkin scores; catching a rock costs a life; a
 * pumpkin that falls off the bottom costs a life; a rock that falls off
 * the bottom is a clean dodge and costs nothing.
 */
function resolve(state) {
  const { config } = state;
  let { score, lives } = state;
  const objects = [];
  for (const object of state.objects) {
    if (overlapsBasket(object, state.basketX, config)) {
      if (object.kind === 'pumpkin') score += config.pumpkinPoints;
      else lives -= 1;
    } else if (object.y - config.objectRadius <= config.height) {
      objects.push(object);
    } else if (object.kind === 'pumpkin') {
      lives -= 1;
    }
  }
  return {
    ...state,
    score,
    lives: Math.max(0, lives),
    status: lives <= 0 ? 'over' : 'playing',
    objects,
  };
}
