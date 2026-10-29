# Welcome to your organization's demo respository
This code repository (or "repo") is designed to demonstrate the best GitHub has to offer with the least amount of noise.

The repo includes an `index.html` file (so it can render a web page), two GitHub Actions workflows, and a CSS stylesheet dependency.

## Pumpkin Atelier

`index.html` is a one-page, whimsical Halloween site for artsy folks. The cursor becomes a paintbrush that flings little pumpkins as it moves; click (or tap) for a burst.

- `assets/style.css` — layout, palette and the brush cursor styles.
- `assets/brush.js` — brush cursor + pumpkin particles (reuses the `#pumpkin` / `#face-*` SVG symbols in `index.html`; honours `prefers-reduced-motion`).

No build step. Preview locally with `python3 -m http.server` and open http://localhost:8000.
