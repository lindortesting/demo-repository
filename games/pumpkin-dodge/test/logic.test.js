/**
 * Tests for the Pumpkin Dodge rules (../logic.js). Run with `node --test`.
 * Every test encodes one game requirement; the doc comment says which.
 */
import test from 'node:test';
import assert from 'node:assert/strict';

import {
  DEFAULT_CONFIG as C,
  createGame,
  step,
  spawnInterval,
  fallSpeed,
  spawnObject,
  overlapsBasket,
} from '../logic.js';

const NO_INPUT = Object.freeze({ left: false, right: false });
const RIGHT = Object.freeze({ left: false, right: true });
const LEFT = Object.freeze({ left: true, right: false });
const DT = 0.05;

/** Deterministic PRNG (mulberry32) so tests are reproducible. */
function seeded(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** A fresh game with spawning paused, isolating movement from the field. */
const idleGame = () => ({ ...createGame(C), spawnTimer: 1e6 });

/** A fresh game with one object placed on the field and spawning paused. */
function gameWithObject(kind, x, y, speed = 0) {
  return { ...idleGame(), objects: [{ kind, x, y, speed }] };
}

const atBasket = (kind) => gameWithObject(kind, C.width / 2, C.height - C.basketHeight);
const belowScreen = (kind) => gameWithObject(kind, 20, C.height + C.objectRadius + 1);

test('a new game starts playing with zero score, full lives, a centered basket and an empty field', () => {
  const state = createGame(C);
  assert.equal(state.status, 'playing');
  assert.equal(state.score, 0);
  assert.equal(state.lives, C.startLives);
  assert.equal(state.basketX, C.width / 2);
  assert.deepEqual(state.objects, []);
  assert.equal(state.spawnTimer, C.spawnStart);
});

test('holding right moves the basket right at basketSpeed', () => {
  const state = step(createGame(C), DT, seeded(1), RIGHT);
  assert.equal(state.basketX, C.width / 2 + C.basketSpeed * DT);
});

test('holding left moves the basket left at basketSpeed', () => {
  const state = step(createGame(C), DT, seeded(1), LEFT);
  assert.equal(state.basketX, C.width / 2 - C.basketSpeed * DT);
});

test('the basket stops at the walls: it can never leave the play field', () => {
  let state = idleGame();
  for (let i = 0; i < 200; i++) state = step(state, DT, seeded(2), RIGHT);
  assert.equal(state.basketX, C.width - C.basketWidth / 2);
  for (let i = 0; i < 400; i++) state = step(state, DT, seeded(3), LEFT);
  assert.equal(state.basketX, C.basketWidth / 2);
});

test('no key held leaves the basket where it is', () => {
  const before = createGame(C);
  const after = step(before, DT, seeded(4), NO_INPUT);
  assert.equal(after.basketX, before.basketX);
});

test('spawns get more frequent as the game progresses, but never faster than spawnMin', () => {
  assert.ok(spawnInterval(30, C) < spawnInterval(0, C));
  assert.equal(spawnInterval(0, C), C.spawnStart);
  assert.equal(spawnInterval(1e6, C), C.spawnMin);
});

test('objects fall faster as the game progresses, but never faster than speedMax', () => {
  assert.ok(fallSpeed(30, C) > fallSpeed(0, C));
  assert.equal(fallSpeed(0, C), C.speedStart);
  assert.equal(fallSpeed(1e6, C), C.speedMax);
});

test('objects spawn just above the top edge and always inside the side walls', () => {
  const rng = seeded(5);
  for (let i = 0; i < 500; i++) {
    const object = spawnObject(0, rng, C);
    assert.equal(object.y, -C.objectRadius);
    assert.ok(object.x >= C.objectRadius);
    assert.ok(object.x <= C.width - C.objectRadius);
  }
});

test('pumpkinChance decides the kind: rng below it spawns a pumpkin, otherwise a rock', () => {
  assert.equal(spawnObject(0, () => C.pumpkinChance - 0.01, C).kind, 'pumpkin');
  assert.equal(spawnObject(0, () => C.pumpkinChance, C).kind, 'rock');
});

test('an object spawns once the spawn timer expires, and the timer resets to the current interval', () => {
  const before = { ...createGame(C), spawnTimer: 0.01 };
  const after = step(before, DT, seeded(6), NO_INPUT);
  assert.equal(after.objects.length, 1);
  assert.equal(after.spawnTimer, spawnInterval(DT, C));
});

test('catching a pumpkin scores pumpkinPoints and removes it from the field', () => {
  const after = step(atBasket('pumpkin'), DT, seeded(7), NO_INPUT);
  assert.equal(after.score, C.pumpkinPoints);
  assert.equal(after.lives, C.startLives);
  assert.deepEqual(after.objects, []);
});

test('catching a rock costs one life, scores nothing and removes the rock', () => {
  const after = step(atBasket('rock'), DT, seeded(8), NO_INPUT);
  assert.equal(after.score, 0);
  assert.equal(after.lives, C.startLives - 1);
  assert.deepEqual(after.objects, []);
});

test('a pumpkin that falls off the bottom costs one life: missed pumpkins are punished', () => {
  const after = step(belowScreen('pumpkin'), DT, seeded(9), NO_INPUT);
  assert.equal(after.lives, C.startLives - 1);
  assert.equal(after.score, 0);
  assert.deepEqual(after.objects, []);
});

test('a rock that falls off the bottom is a clean dodge: no life lost, no score', () => {
  const after = step(belowScreen('rock'), DT, seeded(10), NO_INPUT);
  assert.equal(after.lives, C.startLives);
  assert.equal(after.score, 0);
  assert.deepEqual(after.objects, []);
});

test('the game ends when the last life is lost', () => {
  const before = { ...atBasket('rock'), lives: 1 };
  const after = step(before, DT, seeded(11), NO_INPUT);
  assert.equal(after.status, 'over');
  assert.equal(after.lives, 0);
});

test('a finished game is final: stepping it again returns the exact same state', () => {
  const over = step({ ...atBasket('rock'), lives: 1 }, DT, seeded(12), NO_INPUT);
  assert.equal(step(over, DT, seeded(13), RIGHT), over);
});

test('step is pure: the input state is never mutated', () => {
  const before = gameWithObject('pumpkin', C.width / 2, 100, 200);
  const snapshot = structuredClone(before);
  step(before, DT, seeded(14), RIGHT);
  assert.deepEqual(before, snapshot);
});

test('overlapsBasket treats the basket as a rectangle the circular object must touch', () => {
  const top = C.height - C.basketHeight;
  const touching = { kind: 'pumpkin', x: C.width / 2, y: top - C.objectRadius, speed: 0 };
  const onePixelAway = { ...touching, y: top - C.objectRadius - 1 };
  assert.ok(overlapsBasket(touching, C.width / 2, C));
  assert.ok(!overlapsBasket(onePixelAway, C.width / 2, C));
  const beside = { kind: 'pumpkin', x: C.width / 2 + C.basketWidth / 2 + C.objectRadius + 1, y: top, speed: 0 };
  assert.ok(!overlapsBasket(beside, C.width / 2, C));
});

test('the same seed and the same inputs replay the exact same game', () => {
  const play = () => {
    const rng = seeded(99);
    let state = createGame(C);
    for (let i = 0; i < 600; i++) {
      const input = i % 3 === 0 ? RIGHT : i % 3 === 1 ? LEFT : NO_INPUT;
      state = step(state, DT, rng, input);
    }
    return state;
  };
  assert.deepEqual(play(), play());
});
