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

FROM python:3.8

ENV OTTERTUNE_OVERRIDE_SERVER_URL="https://api.ottertune.com"

RUN mkdir -p /ottertune/driver
COPY . /ottertune/driver
WORKDIR /ottertune/driver
RUN cp /usr/lib/ssl/openssl.cnf /usr/lib/ssl/openssl_cipher1.cnf && \
    sed -i "s/\(CipherString *= *\).*/\1DEFAULT@SECLEVEL=1 /" "/usr/lib/ssl/openssl_cipher1.cnf" && \
    sed -i "s/\(MinProtocol *= *\).*/\1TLSv1 /" "/usr/lib/ssl/openssl_cipher1.cnf"

RUN pip install -r requirements.txt

CMD python3 -m driver.main --config ./driver/config/driver_config.yaml --aws-region $AWS_REGION --db-identifier $OTTERTUNE_DB_IDENTIFIER  --db-username $OTTERTUNE_DB_USERNAME --db-password $OTTERTUNE_DB_PASSWORD --api-key $OTTERTUNE_API_KEY --db-key $OTTERTUNE_DB_KEY --organization-id $OTTERTUNE_ORG_ID --override-server-url $OTTERTUNE_OVERRIDE_SERVER_URL
