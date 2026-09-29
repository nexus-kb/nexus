This repo contains Nexus-KB, a knowledge base for linux kernel developers. It
is built using Swift 6, Vapor 4, and uses ParadeDB with PG 18 as the database.
The lkml lore public-inbox archives must be stored under /opt/nexus/lore.

For WebUI visual changes, run `pnpm dev` in `WebUI` (Vite proxies `/api` to
`127.0.0.1:8080`) and use Playwright to exercise representative states in
Chromium, Firefox, and WebKit. Capture and inspect screenshots from each engine;
WebKit is the Safari compatibility check.

When running on amp orbs, you must coordinate any backend changes with a fresh
thread using the nexus-dev runner since the database and the api server work is 
done in another environment and the orb connects to it via tailscale. 