IMAGE_TAG := $(shell git rev-parse --short HEAD)$(shell git diff-index --quiet HEAD -- || printf '%s' -DIRTY)
IMAGE_REPO := us-central1-docker.pkg.dev/molten-verve-216720/supabase-repository/
ORIOLEDB_REF ?= main
ORIOLEDB_REF_TAG := $(subst /,-,$(ORIOLEDB_REF))
PG_MAJOR ?= 17

export IMAGE_TAG IMAGE_REPO ORIOLEDB_REF ORIOLEDB_REF_TAG PG_MAJOR

CFG ?= workload/jepsen-repeatable-read

.DEFAULT_GOAL := help

.PHONY: help # Generate list of targets with descriptions
help:
	@grep '^.PHONY: .* #' Makefile | sed 's/\.PHONY: \(.*\) # \(.*\)/\1\t\2/' | expand -t20

.PHONY: config # print reified configs
config:
	config/make-compose $(CFG)

.PHONY: build # Build Docker images
build:
	config/make-compose $(CFG) | docker compose -f - build
	config/make-image $(CFG)

.PHONY: up # Build Docker images
up:
	config/make-compose $(CFG) | docker compose -f - up --pull=build --detach

.PHONY: push # push simulation images
push:
	config/make-compose $(CFG) | docker compose -f - push
	config/make-image --push $(CFG)

.PHONY: down # Stop Docker services, remove volumes, delete intermediate files
down:
	config/make-compose $(CFG) | docker compose -f - down --volumes
	rm -rf ./target
