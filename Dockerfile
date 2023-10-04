# Runtime Environment variables required:
#
# AWS_REGION - aws region that the your database is in
# OTTERTUNE_DB_IDENTIFIER - aws rds db identifier for your database
# OTTERTUNE_DB_USERNAME - username to access database
# OTTERTUNE_DB_PASSWORD - password to access database
# OTTERTUNE_API_KEY - secret key granted from ottertune.com
# OTTERTUNE_DB_KEY - db identifier key from ottertune.com
# OTTERTUNE_ORG_ID - organization id from ottertune.com
#
# Postgres only:
# POSTGRES_OTTERTUNE_DB_NAME - Specific database in DBMS to collect metrics from

FROM python:3.9.9-slim-bullseye

ENV OTTERTUNE_OVERRIDE_SERVER_URL="https://api.ottertune.com"
ENV OTTERTUNE_OVERRIDE_NUM_TABLE_TO_COLLECT_STATS="1000"
ENV OTTERTUNE_OVERRIDE_NUM_INDEX_TO_COLLECT_STATS="10000"
ENV OTTERTUNE_OVERRIDE_TABLE_LEVEL_MONITOR_INTERVAL="3600"
ENV OTTERTUNE_DISABLE_TABLE_LEVEL_STATS="False"
ENV OTTERTUNE_DISABLE_INDEX_STATS="False"
ENV OTTERTUNE_DISABLE_QUERY_MONITORING="False"
ENV OTTERTUNE_DISABLE_LONG_RUNNING_QUERY_MONITORING="False"
ENV OTTERTUNE_OVERRIDE_QUERY_MONITOR_INTERVAL="3600"
ENV OTTERTUNE_OVERRIDE_NUM_QUERY_TO_COLLECT="10000"
ENV OTTERTUNE_DISABLE_SCHEMA_MONITORING="False"
ENV OTTERTUNE_OVERRIDE_SCHEMA_MONITOR_INTERVAL="3600"
ENV OTTERTUNE_ENABLE_AWS_IAM_AUTH="False"
ENV OTTERTUNE_OVERRIDE_LONG_RUNNING_QUERY_MONITOR_INTERVAL="120"
ENV OTTERTUNE_OVERRIDE_LR_QUERY_LATENCY_THRESHOLD_MIN="2"
ENV OTTERTUNE_ENABLE_S3="False"
ENV OTTERTUNE_AGENT_HEALTH_REPORT_INTERVAL="60"


RUN   apt-get clean \
   && apt-get update \
   && apt-get install -yq gcc musl-dev python3-dev libpq-dev g++
RUN cp /usr/lib/ssl/openssl.cnf /usr/lib/ssl/openssl_cipher1.cnf && \
    sed -i "s/\(CipherString *= *\).*/\1DEFAULT@SECLEVEL=1 /" "/usr/lib/ssl/openssl_cipher1.cnf" && \
    sed -i "s/\(MinProtocol *= *\).*/\1TLSv1 /" "/usr/lib/ssl/openssl_cipher1.cnf"

RUN mkdir -p /ottertune/driver
WORKDIR /ottertune/driver
# Only copy over requirements.txt so we can take advtantage of caching the
# dependency installation steps.
COPY ./requirements.txt /ottertune/driver/requirements.txt

RUN pip install -r requirements.txt

# Add source after installing deps to make iterating faster
COPY . /ottertune/driver

CMD python3 -m driver.main --config ./driver/config/driver_config.yaml --aws-region $AWS_REGION --db-identifier $OTTERTUNE_DB_IDENTIFIER  --db-username $OTTERTUNE_DB_USERNAME --db-password $OTTERTUNE_DB_PASSWORD --api-key $OTTERTUNE_API_KEY --db-key $OTTERTUNE_DB_KEY --organization-id $OTTERTUNE_ORG_ID --override-server-url $OTTERTUNE_OVERRIDE_SERVER_URL \
  --override-num-table-to-collect-stats $OTTERTUNE_OVERRIDE_NUM_TABLE_TO_COLLECT_STATS \
  --override-table-level-monitor-interval $OTTERTUNE_OVERRIDE_TABLE_LEVEL_MONITOR_INTERVAL \
  --disable-table-level-stats $OTTERTUNE_DISABLE_TABLE_LEVEL_STATS \
  --override-num-index-to-collect-stats $OTTERTUNE_OVERRIDE_NUM_INDEX_TO_COLLECT_STATS \
  --disable-index-stats $OTTERTUNE_DISABLE_INDEX_STATS \
  --disable-query-monitoring $OTTERTUNE_DISABLE_QUERY_MONITORING \
  --disable-long-running-query-monitoring $OTTERTUNE_DISABLE_LONG_RUNNING_QUERY_MONITORING \
  --override-query-monitor-interval $OTTERTUNE_OVERRIDE_QUERY_MONITOR_INTERVAL \
  --override-num-query-to-collect $OTTERTUNE_OVERRIDE_NUM_QUERY_TO_COLLECT \
  --disable-schema-monitoring $OTTERTUNE_DISABLE_SCHEMA_MONITORING \
  --override-schema-monitor-interval $OTTERTUNE_OVERRIDE_SCHEMA_MONITOR_INTERVAL \
  --enable-aws-iam-auth $OTTERTUNE_ENABLE_AWS_IAM_AUTH \
  --override-long-running-query-monitor-interval $OTTERTUNE_OVERRIDE_LONG_RUNNING_QUERY_MONITOR_INTERVAL \
  --override-lr-query-latency-threshold-min $OTTERTUNE_OVERRIDE_LR_QUERY_LATENCY_THRESHOLD_MIN \
  --enable-s3 $OTTERTUNE_ENABLE_S3 \
  --agent-health-report-interval $OTTERTUNE_AGENT_HEALTH_REPORT_INTERVAL
