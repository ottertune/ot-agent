# OtterTune Agent - metric collector for external databases.

[![CircleCI](https://circleci.com/gh/ottertune/ot-agent/tree/main.svg?style=svg&circle-token=371db20e018f2af9c286f96230a18d178657d9a1)](https://circleci.com/gh/ottertune/ot-agent/tree/main)

<p align="center">
  <img src="https://user-images.githubusercontent.com/3093192/135324592-ec28dc1a-4542-45c3-b371-76e30c0e375b.png">
</p>

# Support / Documentation

- Quick Start: https://docs.ottertune.com/support/kb/categories/amRZXxmJ/getting-started
- Agent Setup: https://docs.ottertune.com/support/kb/articles/A93xgYW0/agent-setup
- [Reach out on Slack!](https://join.slack.com/t/ottertune-community/shared_invite/zt-wr4gztk0-Sta_86xRQ6~o3WRpMvRlgA)

----

# Development

## Setup

To run the Agent, you will need an environment with `Python 3.8+`.

Install python dependencies:
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

### Optional arguments
```
--disable-index-stats: Disable index stats collection (default: False).
--disable-query-monitoring: Disable query monitoring (default: False).
--disable-long-running-query-monitoring: Disable long running query monitoring (default: False).
--disable-schema-monitoring: Disable schema monitoring (default: False).
--disable-table-level-stats: Disable collecting stats for table level analysis (default: False).
--enable-aws-iam-auth: Use AWS IAM authentication when connecting to the RDS instance (default: False).
--log-verbosity: Set the logging level (default: INFO).
--override-monitor-interval: Override the frequency of collecting new data (in seconds).
--override-num-index-to-collect-stats: Override the number of indexes to collect stats.
--override-num-query-to-collect: Override the number of queries to collect.
--override-num-table-to-collect-stats: Override the number of tables to collect table level stats.
--override-query-monitor-interval: Override the frequency of collecting query data (in seconds).
--override-schema-monitor-interval: Override the frequency of collecting schema data (in seconds).
--override-server-url: Override the endpoint to post observation data.
--override-table-level-monitor-interval: Override the frequency of collecting table level data (in seconds).
--override-long-running-query-monitor-interval: Override the frequency of collecting long running query data (in seconds).
```

## Permissions
To collect metrics from the database, you need to [configure database settings](https://docs.ottertune.com/support/kb/articles/wmjvaGmV/enabling-database-instances), and [grant permissions](https://docs.ottertune.com/support/kb/articles/j9bXRMmn/agent-database-permissions) to the database user.

## Makefile

| make all                    | to run all the necessary checks before submitting your PR, which includes format, test, pylint, and pyre.                                                                                                                                                                                                                                                                    |
|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| make format                 | to run an autoformatter                                                                                                                                                                                                                                                                                                                                                      |
| make test                   | to run all unit tests                                                                                                                                                                                                                                                                                                                                                        |
| make lint                   | to run linter                                                                                                                                                                                                                                                                                                                                                                |
| make typecheck              | to run pyre typechecker                                                                                                                                                                                                                                                                                                                                                      |
| make docker [VERSION=0.1.0] | to build the docker image with provided tag (default: `ottertune/agent:0.1.0`).                                                                                                                                                                                                                                                                                              |
| make publish                | builds the Docker image for `linux/amd64` and `linux/arm64`, then uploads the images to Dockerhub. To use, you must first run create a Docker builder using the following command:  ```bash docker buildx create --name mybuilder --use ```  `mybuilder` can be any name. You only need to run this command once; after which, you can run `make publish` whenever you want. |

## Code Flow

A basic diagram showing the logic flow of different components:
https://www.figma.com/file/SwvHqpya7BGBj9MHot6dGC/OT-Agent?node-id=0%3A1

<img width="1295" alt="Screen Shot 2022-08-10 at 11 39 45 AM" src="https://user-images.githubusercontent.com/5196925/183947336-b0743c9e-2a75-4cfe-af4a-c81bbdecbd6f.png">

----

# Deploying a Release

Make sure the value in the code for the agent version number is correct: https://github.com/ottertune/ot-agent/blob/main/driver/compute_server_client.py#L23

Create a release by [making one in the Github UI](https://github.com/ottertune/ot-agent/releases/new). This will automatically trigger a job to build and push images to public ECR and Dockerhub. Each version name is a two word alliteration of the form: Adjective Animal.

Once you've deployed the release, please update the fixture located in:

[service/backend/settings/fixtures/agent_release.json](https://github.com/ottertune/service/blob/develop/backend/settings/fixtures/agent_release.json)

Also update the agent helm chart:
[agent-helm-chart/Chart.yaml](https://github.com/ottertune/agent-helm-chart/blob/main/Chart.yaml)
Then create a release by [making one in the Github UI](https://github.com/ottertune/agent-helm-chart/releases/new)
This will utomatically trigger a job to build and push images to support installing agent via Helm Chart

Also update the documentation [here]( https://docs.ottertune.com/support/kb/articles/496D50mX/agent-upgrade) and [here](https://docs.ottertune.com/support/kb/articles/A93xgYW0/agent-setup) :



----

# License

See https://github.com/ottertune/ot-agent/blob/main/LICENSE
