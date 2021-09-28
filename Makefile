VERSION := 0.1.0
REPO := ottertune/agent
VERSION_TAG := $(REPO):$(VERSION)
LATEST_TAG := $(REPO):latest

.PHONY: docker

all: format test lint typecheck

# Format will run an autoformatter over the Python code.
format:
	python3 -m black driver/ tests/

# Typecheck will run the Pyre typechecker over the source code.
typecheck:
	pyre

# Lint runs the linter. This is requred to pass CI.
lint:
	pylint driver/ tests/

# Test runs Pytest. This is required to pass CI.
test:
	python3 -m pytest

# Build a Docker image for local use.
docker: 
	docker build . -t $(VERSION_TAG) -t $(LATEST_TAG)

# Publish a new Docker image to Dockerhub.
publish:
	docker buildx build --platform linux/amd64,linux/arm64 -t $(VERSION_TAG) -t $(LATEST_TAG) --push .
