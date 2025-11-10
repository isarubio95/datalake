# docker/dbt-runner/Dockerfile
# syntax=docker/dockerfile:1.6
ARG PYTHON_IMAGE=python:3.10-slim-bookworm
FROM ${PYTHON_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ### CAMBIO: Se añade 'procps' a la lista de paquetes ###
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential curl ca-certificates gnupg \
      openjdk-17-jre-headless \
      procps \
      && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN test -d "$JAVA_HOME"

WORKDIR /opt
COPY requirements.txt /opt/requirements.txt
RUN pip install --upgrade pip && pip install -r /opt/requirements.txt

WORKDIR /work