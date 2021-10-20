# OtterTune Agent - metric collector for external databases.

[![CircleCI](https://circleci.com/gh/ottertune/ot-agent/tree/main.svg?style=svg&circle-token=371db20e018f2af9c286f96230a18d178657d9a1)](https://circleci.com/gh/ottertune/ot-agent/tree/main)

<p align="center">
  <img src="https://user-images.githubusercontent.com/3093192/135324592-ec28dc1a-4542-45c3-b371-76e30c0e375b.png">
</p>

# Support / Documentation

- Quick Start: https://docs.ottertune.com/#getting-started
- Agent Setup: https://docs.ottertune.com/info/connect-your-database-to-ottertune/add-database/agent
- [Reach out on Slack!](https://join.slack.com/t/ottertune-community/shared_invite/zt-wr4gztk0-Sta_86xRQ6~o3WRpMvRlgA)

----

# Development

## Setup

To install python dependencies:
```
pip install -r requirements.txt
```

Ensure your aws env vars are set: `AWS_ACCESS_KEY_ID` `AWS_SECRET_ACCESS_KEY`

## Tests

```
python3 -m pytests .
```

## Running Locally

To test your code running locally, set up a database in dev and run against it.

```
python3 -m driver.main --aws-region <region> --db-identifier <db-identifier-with-public-access> --db-username <username> --db-password <password> --api-key <dev_api_key> --db-key <given_db_key> --organization-id <dev_org_id> --config driver/config/driver_config.yaml --override-server-url https://dev.api.ottertune.com
```

If you are using postgres you will need to set the `POSTGRES_OTTERTUNE_DB_NAME` env variable if you are using a postgres database.

## Makefile

| make all                    | to run all the necessary checks before submitting your PR, which includes format, test, pylint, and pyre.                                                                                                                                                                                                                                                                    |
|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| make format                 | to run an autoformatter                                                                                                                                                                                                                                                                                                                                                      |
| make test                   | to run all unit tests                                                                                                                                                                                                                                                                                                                                                        |
| make lint                   | to run linter                                                                                                                                                                                                                                                                                                                                                                |
| make typecheck              | to run pyre typechecker                                                                                                                                                                                                                                                                                                                                                      |
| make docker [VERSION=0.1.0] | to build the docker image with provided tag (default: `ottertune/agent:0.1.0`).                                                                                                                                                                                                                                                                                              |
| make publish                | builds the Docker image for `linux/amd64` and `linux/arm64`, then uploads the images to Dockerhub. To use, you must first run create a Docker builder using the following command:  ```bash docker buildx create --name mybuilder --use ```  `mybuilder` can be any name. You only need to run this command once; after which, you can run `make publish` whenever you want. |

----

# Deploying a Release

Create a release by [making one in the Github UI](https://github.com/ottertune/ot-agent/releases/new). This will automatically trigger a job to build and push images to public ECR and Dockerhub. Currently each version is named after a character from the show [The Wire](https://en.wikipedia.org/wiki/The_Wire).

----

# License

See https://github.com/ottertune/ot-agent/blob/main/LICENSE
