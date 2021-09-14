# Runtime Environment variables required:
#
# OTTERTUNE_DB_TYPE eg: postgres, mysql
# OTTERTUNE_API_KEY - secret key granted from ottertune.com
# OTTERTUNE_DB_KEY - db identifier key from ottertune.com
# OTTERTUNE_DB_USERNAME - username to access database
# OTTERTUNE_DB_PASSWORD - password to access database
# OTTERTUNE_HOSTNAME - hostname of the database
# OTTERTUNE_PORT - port of the database

FROM python:3.8

ARG CODEARTIFACT_AUTH_TOKEN

RUN mkdir -p /ottertune/driver
COPY . /ottertune/driver
WORKDIR /ottertune/driver

# codeartifact login
RUN pip install awscli==1.19
RUN pip config set global.index-url https://aws:$CODEARTIFACT_AUTH_TOKEN@ottertune-691523222388.d.codeartifact.us-east-2.amazonaws.com/pypi/pypi-store/simple/

RUN pip install -r requirements.txt
RUN pip uninstall -y awscli

CMD python3 -m driver.main --deployment onprem --config ./driver/config/driver_config.yaml --override-db-type $OTTERTUNE_DB_TYPE --override-api-key $OTTERTUNE_API_KEY --override-db-key $OTTERTUNE_DB_KEY --override-db-username $OTTERTUNE_DB_USERNAME --override-db-password $OTTERTUNE_DB_PASSWORD --override-hostname $OTTERTUNE_HOSTNAME --override-port $OTTERTUNE_PORT
