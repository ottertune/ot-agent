# OtterTune Agent - metric collector for external databases.

[![CircleCI](https://circleci.com/gh/ottertune/driver.svg?style=svg&circle-token=a6bcd60de064fb1b0a03861f918f023685de2020)](https://app.circleci.com/pipelines/github/ottertune/driver)

<p align="center">
  <img src="https://user-images.githubusercontent.com/3093192/135324592-ec28dc1a-4542-45c3-b371-76e30c0e375b.png">
</p>

# Support / Documentation

- Quick Start: https://docs.ottertune.com/#getting-started
- Agent Setup: https://docs.ottertune.com/info/connect-your-database-to-ottertune/add-database/agent
- [Reach out on Slack!](https://join.slack.com/t/ottertune-community/shared_invite/zt-wr4gztk0-Sta_86xRQ6~o3WRpMvRlgA)

----

# Developement

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

----

# License

See https://github.com/ottertune/ot-agent/blob/main/LICENSE
