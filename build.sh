#!/bin/bash
set -e

# Make sure we're ignoring any override files that may be present
export COMPOSE_FILE='docker-compose.yml'

docker-compose build
docker-compose run --rm app $@
