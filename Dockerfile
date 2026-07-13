# Utiliza a imagem que você já começou a baixar
FROM apache/airflow:slim-latest-python3.12

# Copia o requirements para dentro do contentor
COPY requirements.txt /requirements.txt

# Instala as dependências como root/airflow user
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt