FROM python:3.9.11-alpine3.15 as base

# build and load all requirements
FROM base as builder
WORKDIR /airbyte/integration_code
COPY .venv ./
# upgrade pip to the latest version
RUN apk --no-cache upgrade \
    && python -m venv .venv && source .venv/bin/activate  \
    && apk --no-cache add tzdata build-base

COPY setup.py ./
COPY requirements.txt ./

# build a clean environment
FROM base
WORKDIR /airbyte/integration_code

# # copy all loaded and built libraries to a pure basic image
COPY --from=builder /airbyte/integration_code/.venv /usr/local/.venv
# add default timezone settings
COPY --from=builder /usr/share/zoneinfo/Etc/UTC /etc/localtime
RUN echo "Etc/UTC" > /etc/timezone

# bash is installed for more convenient debugging.
RUN apk --no-cache add bash

# copy payload code only
COPY main.py ./
COPY destination_databend_py ./destination_databend_py

ENV AIRBYTE_ENTRYPOINT "/usr/local/.venv/python /airbyte/integration_code/main.py"
ENTRYPOINT ["/usr/local/.venv/python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=airbyte/destination-databend-py
