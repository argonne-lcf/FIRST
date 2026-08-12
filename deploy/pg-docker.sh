#!/bin/bash

set -eu
docker run --name inference-gateway-db -e POSTGRES_PASSWORD=dataportaldevpwd123 -e POSTGRES_USER=dataportaldev -e POSTGRES_DB=postgres -p 6432:5432 -d postgres
