VERSION := 0.1.0
TAG := ottertune/agent:$(VERSION)

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

docker: 
	docker build . -t $(TAG) -t latest
