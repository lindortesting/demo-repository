/*
 * Pumpkin brush: replaces the mouse cursor with a paintbrush and spits out
 * little pumpkins as it moves (or as a finger drags on touch screens).
 * Clicking/tapping makes a burst. Particles reuse the #pumpkin and #face-*
 * SVG symbols defined in index.html.
 */
(() => {
  const PALETTE = ["#f28c38", "#f4c95d", "#b9a2e0", "#8fc9a8", "#ff9e7a", "#e0742a", "#ffb3c7"];
  const FACES = ["happy", "sleepy", "wink", "oh"];
  const SPACING = 30;   // px of brush travel per pumpkin
  const MAX_ALIVE = 90; // keep the DOM light on frantic scribbling
  const reducedMotion = matchMedia("(prefers-reduced-motion: reduce)");

  const brush = document.querySelector(".brush");
  const pick = (list) => list[Math.floor(Math.random() * list.length)];
  const rand = (min, max) => min + Math.random() * (max - min);

  let nextColor = pick(PALETTE);
  let alive = 0;
  let last = null;
  let travelled = 0;

  // Ballistic arc sampled into keyframes: launched along (dx, dy), pulled down by gravity.
  const flightFrames = (x, y, dx, dy, size, spin) => {
    const speed = rand(60, 160);
    const len = Math.hypot(dx, dy) || 1;
    const vx = (dx / len) * speed * 0.6 + rand(-70, 70);
    const vy = (dy / len) * speed * 0.6 - rand(90, 170);
    return Array.from({ length: 9 }, (_, i) => {
      const t = i / 8;
      const px = x + vx * t - size / 2;
      const py = y + vy * t + 320 * t * t - size / 2;
      const scale = i === 0 ? 0 : i === 1 ? 1.2 : 1;
      return {
        transform: `translate(${px}px, ${py}px) rotate(${spin * t}deg) scale(${scale})`,
        opacity: t < 0.7 ? 1 : 1 - (t - 0.7) / 0.3,
      };
    });
  };

  const stillFrames = (x, y, size) => [
    { transform: `translate(${x - size / 2}px, ${y - size / 2}px)`, opacity: 0 },
    { transform: `translate(${x - size / 2}px, ${y - size / 2}px)`, opacity: 1, offset: 0.2 },
    { transform: `translate(${x - size / 2}px, ${y - size / 2}px)`, opacity: 0 },
  ];

  const spawn = (x, y, dx = 0, dy = -1) => {
    if (alive >= MAX_ALIVE) return;
    const size = rand(24, 48);
    const el = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    el.setAttribute("class", "pumpkin-particle");
    el.setAttribute("viewBox", "0 0 100 100");
    el.setAttribute("width", size);
    el.setAttribute("height", size);
    el.style.color = nextColor;
    el.innerHTML = `<use href="#pumpkin"/><use href="#face-${pick(FACES)}"/>`;
    document.body.append(el);
    alive++;

    const frames = reducedMotion.matches
      ? stillFrames(x, y, size)
      : flightFrames(x, y, dx, dy, size, rand(-200, 200));
    el.animate(frames, { duration: rand(1300, 1900), easing: "linear" }).onfinish = () => {
      el.remove();
      alive--;
    };

    nextColor = pick(PALETTE);
    brush.style.setProperty("--tip", nextColor);
  };

  const moveBrush = (e) => {
    brush.style.setProperty("--x", `${e.clientX}px`);
    brush.style.setProperty("--y", `${e.clientY}px`);
    brush.classList.toggle("is-visible", e.pointerType !== "touch");
  };

  // Only take over the cursor on devices with a real pointer.
  if (matchMedia("(pointer: fine)").matches) document.body.classList.add("has-brush");
  brush.style.setProperty("--tip", nextColor);

  addEventListener("pointermove", (e) => {
    moveBrush(e);
    if (e.pointerType === "touch" && !e.isPrimary) return;
    const here = { x: e.clientX, y: e.clientY };
    if (last) {
      const dx = here.x - last.x;
      const dy = here.y - last.y;
      travelled += Math.hypot(dx, dy);
      if (travelled >= SPACING) {
        travelled = 0;
        spawn(here.x, here.y, dx, dy);
      }
    }
    last = here;
  }, { passive: true });

  addEventListener("pointerdown", (e) => {
    moveBrush(e);
    last = { x: e.clientX, y: e.clientY };
    brush.classList.add("is-pressed");
    const burst = reducedMotion.matches ? 1 : 7;
    for (let i = 0; i < burst; i++) spawn(e.clientX, e.clientY, rand(-1, 1), -1);
  });

  addEventListener("pointerup", () => brush.classList.remove("is-pressed"));

  // Hide the brush (and reset the stroke) when the pointer leaves the window.
  document.documentElement.addEventListener("pointerleave", () => {
    brush.classList.remove("is-visible");
    last = null;
  });
})();
