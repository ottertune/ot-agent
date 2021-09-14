CODEARTIFACT_DOMAIN_OWNER := 691523222388
CODEARTIFACT_AUTH_TOKEN := $(shell aws codeartifact get-authorization-token --domain ottertune --domain-owner $(CODEARTIFACT_DOMAIN_OWNER) --query authorizationToken --output text)
TAG := ottertune-driver:latest

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

login:
	aws codeartifact login --tool pip --repository pypi-store --domain ottertune --domain-owner $(CODEARTIFACT_DOMAIN_OWNER)
	aws codeartifact login --tool twine --repository pypi-store --domain ottertune --domain-owner $(CODEARTIFACT_DOMAIN_OWNER)

# This command outputs a CodeArtifact token for this session. Use this token
# if you're planning to build a Docker image.
auth-token:
	aws codeartifact get-authorization-token --domain ottertune --domain-owner $(CODEARTIFACT_DOMAIN_OWNER) --query authorizationToken --output text

docker: 
	(docker build \
	--build-arg CODEARTIFACT_DOMAIN_OWNER=$(CODEARTIFACT_DOMAIN_OWNER) \
	--build-arg CODEARTIFACT_AUTH_TOKEN=$(CODEARTIFACT_AUTH_TOKEN) \
	. -t $(TAG))
