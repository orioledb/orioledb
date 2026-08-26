IMAGE_TAG := $(shell git rev-parse --short HEAD)$(shell git diff-index --quiet HEAD -- || printf '%s' -DIRTY)
IMAGE_REPO := $(shell awk -F '"' '/^[[:space:]]*repository[[:space:]]*=/ { sub(/\/+$$/, "", $$2); print $$2 "/"; exit }' .snouty.toml)
ORIOLEDB_REF ?= main
ORIOLEDB_REF_TAG := $(subst /,-,$(ORIOLEDB_REF))
PG_MAJOR ?= 17
PLATFORM ?= linux/amd64

export IMAGE_TAG IMAGE_REPO ORIOLEDB_REF ORIOLEDB_REF_TAG PG_MAJOR PLATFORM

CFG ?= workload/jepsen workload/jepsen-RR

.DEFAULT_GOAL := help

.PHONY: help # Generate list of targets with descriptions
help:
	@grep '^.PHONY: .* #' Makefile | sed 's/\.PHONY: \(.*\) # \(.*\)/\1\t\2/' | expand -t20

.PHONY: config # print reified configs
config:
	@config/make-compose $(CFG)

.PHONY: config-image # print reified configs
config-image:
	@config/print-config-image $(CFG)

ARGS_BUILD ?=
.PHONY: build-services # Build Docker service images
build-services:
	config/make-compose $(CFG) | docker compose -f - build $(ARGS_BUILD)

.PHONY: build-config # Build the Antithesis config image
build-config:
	mkdir -p target && rm -rf target/*; \
	config/make-compose $(CFG) > target/docker-compose.yaml; \
	docker build --platform "$$PLATFORM" -f config/Dockerfile -t "$$(config/print-config-image $(CFG))" .

.PHONY: build # Build Docker images
build: build-services build-config

ARGS_UP ?= --pull=build --detach
.PHONY: up # Build Docker images
up:
	config/make-compose $(CFG) | docker compose -f - up $(ARGS_UP)

.PHONY: push-services # Push simulation service images
push-services:
	@compose="$$(config/make-compose $(CFG))"; \
	services="$$(printf '%s\n' "$$compose" | docker compose -f - config --format json | \
		jq -r --arg repo "$$IMAGE_REPO" '.services | to_entries[] | select((.value.image // "") | startswith($$repo)) | .key')"; \
	if [ -z "$$IMAGE_REPO" ] || [ -z "$$services" ]; then \
		echo "Error: no services target IMAGE_REPO=$$IMAGE_REPO" >&2; \
		exit 1; \
	fi; \
	printf '%s\n' "$$compose" | docker compose -f - push $$services

.PHONY: push-config # Build and push the Antithesis config image
push-config:
	docker push "$$(config/print-config-image $(CFG))"

.PHONY: push # Push simulation images
push: push-services push-config

.PHONY: down # Stop Docker services, remove volumes, delete intermediate files
down:
	config/make-compose $(CFG) | docker compose -f - down --volumes
	rm -rf ./target
