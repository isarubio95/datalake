ARG PYTHON_IMAGE=python:3.10-slim-bookworm
FROM ${PYTHON_IMAGE}

# Evita prompts
ENV DEBIAN_FRONTEND=noninteractive PIP_NO_CACHE_DIR=1

# Dependencias del sistema, usando openjdk-17-jdk
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential default-libmysqlclient-dev curl ca-certificates git \
    libstdc++6 libgfortran5 libopenblas0 liblapack3 \
    openjdk-17-jdk \
 && rm -rf /var/lib/apt/lists/*

# Establecemos la variable de entorno JAVA_HOME para Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Instala requirements
WORKDIR /opt/app
COPY requirements.txt /opt/app/requirements.txt
RUN python -m pip install --upgrade pip && pip install -r /opt/app/requirements.txt

# Prepara usuario y directorios
RUN useradd -ms /bin/bash dagster && mkdir -p /dagster_home && chown -R dagster:dagster /dagster_home
ENV DAGSTER_HOME=/dagster_home

# Trabajo
WORKDIR /work
USER dagster

EXPOSE 3000