# Runtime Environment variables required:
#
# AWS_ACCESS_KEY_ID - access key id for user with rds describe and cloudwatch log access
# AWS_SECRET_ACCESS_KEY - secret access key for user with rds describe and cloudwatch log access
# AWS_REGION - aws region that the your database is in
# OTTERTUNE_DB_IDENTIFIER - aws rds db identifier for your database
# OTTERTUNE_DB_USERNAME - username to access database
# OTTERTUNE_DB_PASSWORD - password to access database
# OTTERTUNE_API_KEY - secret key granted from ottertune.com
# OTTERTUNE_DB_KEY - db identifier key from ottertune.com
# OTTERTUNE_ORG_ID - organization id from ottertune.com
#
# Optional:
# OPTIONAL_OTTERTUNE_DB_NAME - Specific database in DBMS to collect metrics from

FROM python:3.8

ENV OPTIONAL_OTTERTUNE_DB_NAME=""

RUN mkdir -p /ottertune/driver
COPY . /ottertune/driver
WORKDIR /ottertune/driver

RUN pip install -r requirements.txt

CMD python3 -m driver.main --config ./driver/config/driver_config.yaml --aws-region $AWS_REGION --db-identifier $OTTERTUNE_DB_IDENTIFIER  --db-username $OTTERTUNE_DB_USERNAME --db-password $OTTERTUNE_DB_PASSWORD --api-key $OTTERTUNE_API_KEY --db-key $OTTERTUNE_API_KEY --organization-id $OTTERTUNE_ORG_ID --db-name $OPTIONAL_OTTERTUNE_DB_NAME 
