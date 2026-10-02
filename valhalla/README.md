# Valhalla truck routing

Kadads uses an authenticated private Valhalla HTTP server for truck routes. Set `VALHALLA_URL` and `VALHALLA_API_SECRET` privately. Advice is held if a current truck route cannot be verified. Other routing engines and geometric distance estimates do not authorize driver fuel instructions.

`python -m scripts.refresh_stop_graph` refreshes the truck-stop graph against the configured server. Review the result before relying on station access. The optional `scripts/build_valhalla_tiles.sh` helper builds tiles for a separately managed routing server from an operator-provided OSM extract. It requires Valhalla build utilities and enough disk and memory.

Do not commit server keys, OSM extracts, generated tiles, or private server configuration. The application repository does not provision or publish the private routing server.
