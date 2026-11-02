/**
 * Pumpkin Dodge — DOM shell: canvas rendering, keyboard input, HUD.
 * All rules live in `logic.js`; this file only turns state into pixels.
 */
import { DEFAULT_CONFIG as config, createGame, step } from './logic.js';

const canvas = document.getElementById('game');
const ctx = canvas.getContext('2d');
const scoreEl = document.getElementById('score');
const livesEl = document.getElementById('lives');
const overlay = document.getElementById('overlay');
const finalScoreEl = document.getElementById('final-score');
const restartButton = document.getElementById('restart');

canvas.width = config.width;
canvas.height = config.height;

const STARS = [
  [40, 60], [120, 30], [210, 90], [300, 45], [390, 110], [450, 70],
  [75, 180], [260, 160], [420, 205], [160, 245], [350, 262], [30, 320],
];

const keys = { left: false, right: false };
let state = createGame(config);
let last = performance.now();

function trackArrowKey(event, down) {
  if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
  event.preventDefault();
  keys[event.key === 'ArrowLeft' ? 'left' : 'right'] = down;
}

window.addEventListener('keydown', (event) => trackArrowKey(event, true));
window.addEventListener('keyup', (event) => trackArrowKey(event, false));
window.addEventListener('blur', () => {
  keys.left = false;
  keys.right = false;
});

restartButton.addEventListener('click', () => {
  state = createGame(config);
  overlay.hidden = true;
});

function frame(now) {
  const dt = Math.min((now - last) / 1000, 0.05);
  last = now;
  state = step(state, dt, Math.random, keys);
  draw(state);
  scoreEl.textContent = state.score;
  livesEl.textContent = '\u2665'.repeat(state.lives);
  if (state.status === 'over' && overlay.hidden) {
    finalScoreEl.textContent = state.score;
    overlay.hidden = false;
  }
  requestAnimationFrame(frame);
}

function draw(state) {
  const sky = ctx.createLinearGradient(0, 0, 0, config.height);
  sky.addColorStop(0, '#1b1035');
  sky.addColorStop(1, '#3b1d4a');
  ctx.fillStyle = sky;
  ctx.fillRect(0, 0, config.width, config.height);

  ctx.fillStyle = '#f5e6b8';
  ctx.beginPath();
  ctx.arc(config.width - 70, 80, 34, 0, Math.PI * 2);
  ctx.fill();

  ctx.fillStyle = 'rgba(255, 255, 255, 0.7)';
  for (const [x, y] of STARS) {
    ctx.beginPath();
    ctx.arc(x, y, 1.5, 0, Math.PI * 2);
    ctx.fill();
  }

  ctx.fillStyle = '#241436';
  ctx.fillRect(0, config.height - 10, config.width, 10);

  for (const object of state.objects) {
    if (object.kind === 'pumpkin') drawPumpkin(object.x, object.y);
    else drawRock(object.x, object.y);
  }
  drawBasket(state.basketX);
}

function drawPumpkin(x, y) {
  const r = config.objectRadius;
  ctx.fillStyle = '#e8791d';
  ctx.beginPath();
  ctx.ellipse(x, y, r, r * 0.88, 0, 0, Math.PI * 2);
  ctx.fill();

  ctx.strokeStyle = '#c05f10';
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  ctx.ellipse(x, y, r * 0.5, r * 0.88, 0, 0, Math.PI * 2);
  ctx.stroke();

  ctx.fillStyle = '#4c7a34';
  ctx.fillRect(x - 2.5, y - r * 0.88 - 6, 5, 7);

  ctx.fillStyle = '#3a2208';
  for (const eyeX of [x - r * 0.38, x + r * 0.38]) {
    ctx.beginPath();
    ctx.moveTo(eyeX - 3, y - 1);
    ctx.lineTo(eyeX + 3, y - 1);
    ctx.lineTo(eyeX, y - 6);
    ctx.closePath();
    ctx.fill();
  }
  ctx.beginPath();
  ctx.moveTo(x - 6, y + 4);
  ctx.lineTo(x + 6, y + 4);
  ctx.lineTo(x, y + 9);
  ctx.closePath();
  ctx.fill();
}

function drawRock(x, y) {
  const r = config.objectRadius;
  ctx.fillStyle = '#7d7d8a';
  ctx.beginPath();
  for (let i = 0; i < 7; i++) {
    const angle = (i / 7) * Math.PI * 2;
    const rad = r * (0.75 + 0.25 * Math.abs(Math.sin(i * 3.7)));
    const px = x + Math.cos(angle) * rad;
    const py = y + Math.sin(angle) * rad;
    if (i === 0) ctx.moveTo(px, py);
    else ctx.lineTo(px, py);
  }
  ctx.closePath();
  ctx.fill();

  ctx.fillStyle = 'rgba(255, 255, 255, 0.18)';
  ctx.beginPath();
  ctx.arc(x - r * 0.3, y - r * 0.3, r * 0.22, 0, Math.PI * 2);
  ctx.fill();
}

function drawBasket(basketX) {
  const { basketWidth: w, basketHeight: h, height } = config;
  const top = height - h;
  const left = basketX - w / 2;

  ctx.fillStyle = '#a5713a';
  ctx.beginPath();
  ctx.moveTo(left, top);
  ctx.lineTo(left + w, top);
  ctx.lineTo(left + w - 10, top + h);
  ctx.lineTo(left + 10, top + h);
  ctx.closePath();
  ctx.fill();

  ctx.strokeStyle = 'rgba(90, 55, 20, 0.55)';
  ctx.lineWidth = 1.5;
  for (let i = 1; i < 6; i++) {
    const t = i / 6;
    ctx.beginPath();
    ctx.moveTo(left + w * t, top);
    ctx.lineTo(left + 10 + (w - 20) * t, top + h);
    ctx.stroke();
  }
  ctx.beginPath();
  ctx.moveTo(left + 5, top + h / 2);
  ctx.lineTo(left + w - 5, top + h / 2);
  ctx.stroke();

  ctx.fillStyle = '#8a5a2b';
  ctx.fillRect(left - 3, top - 5, w + 6, 6);
}

requestAnimationFrame(frame);
