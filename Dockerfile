# AutoTrack image.
#
# Build context is the repo root, so the COPY paths below are
# relative to the repo root (matching docker-compose.yaml).

FROM apache/airflow:slim-latest-python3.12

# System packages needed at build time only. We install as root,
# then drop back to the airflow user before pip install (the
# airflow image expects pip commands as the airflow user).
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Install Python deps as the airflow user. The image's HOME is
# already set correctly; ``--user`` is not needed.
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Make sure the source tree is on the path even if a developer
# mounts over /opt/airflow/src at runtime. The mount in
# docker-compose.yaml wins, so this is just a safety net.
COPY --chown=airflow:root src /opt/airflow/src
