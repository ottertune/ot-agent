[![CircleCI](https://circleci.com/gh/ottertune/driver.svg?style=svg&circle-token=a6bcd60de064fb1b0a03861f918f023685de2020)](https://app.circleci.com/pipelines/github/ottertune/driver)
# ot-agent
Implements the database collector for external client databases.

## Makefile

In the project directory, you can run:

### `make all`

to run all the necessary checks before submitting your PR, which includes format, test, pylint, and pyre.

### `make format`

to format codes.

### `make test`

to run all the tests inside driver's tests/.

### `make lint`

to run pylint against driver/ and tests/.

### `make typecheck`

to run pyre on driver/ and tests/.

### `make docker [TAG=your_tag:latest]`

to build the docker image with tag (default: `ot-agent:latest`).
