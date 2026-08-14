#!/usr/bin/env bash
# Stand up a throwaway Dozzle log viewer for the local dev stack.
# Dozzle is not part of the compose stack (dev or prod); run this when you want
# a browser log UI, then Ctrl-C to tear it down.
#
#   http://127.0.0.1:8080
#
#   PROJECT   compose project name (default: first, matches .env)
#   PORT      host port to bind    (default: 8080)
set -euo pipefail

PROJECT="${PROJECT:-first}"
PORT="${PORT:-8080}"
NETWORK="${PROJECT}_gateway"

echo "Starting Dozzle on http://127.0.0.1:$PORT (network: $NETWORK). Ctrl-C to stop."
exec docker run --rm -it \
	--name dozzle \
	-v /var/run/docker.sock:/var/run/docker.sock:ro \
	--network "$NETWORK" \
	-p "127.0.0.1:$PORT:8080" \
	amir20/dozzle:latest@sha256:a8441e9d2928cc7b30d0023f5eedbb87ef6e234d87f3be02662bd8f417955b8b
