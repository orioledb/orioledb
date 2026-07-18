IMAGE_TAG := $(shell date -u +%Y%m%dT%H%M%SZ)-$(shell git rev-parse --short HEAD)
IMAGE_REPO := us-central1-docker.pkg.dev/molten-verve-216720/supabase-repository/
ORIOLEDB_REF ?= main
ORIOLEDB_REF_TAG := $(subst /,-,$(ORIOLEDB_REF))
PG_MAJOR ?= 17

export IMAGE_TAG IMAGE_REPO ORIOLEDB_REF ORIOLEDB_REF_TAG PG_MAJOR

CONFIGS ?= setup/s3 workload/jepsen-repeatable-read

.DEFAULT_GOAL := help

.PHONY: help # Generate list of targets with descriptions
help:
	@grep '^.PHONY: .* #' Makefile | sed 's/\.PHONY: \(.*\) # \(.*\)/\1\t\2/' | expand -t20

.PHONY: build # Build Docker images
build:
	config/make-compose $(CONFIGS) | docker compose -f - build
	config/make-image $(CONFIGS)

.PHONY: up # Build Docker images
up:
	config/make-compose $(CONFIGS) | docker compose -f - up -d

.PHONY: clean # Stop Docker services, remove volumes, delete intermediate files
clean:
	config/make-compose $(CONFIGS) | docker compose -f - down --volumes
	rm -rf ./target
