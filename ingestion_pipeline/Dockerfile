FROM apache/airflow:2.10.2
USER root
RUN apt-get update && \
    apt-get install -y \
    poppler-utils \
    gcc \
    libpq-dev \
    libpango-1.0-0 \
    libpangoft2-1.0-0 \
    gir1.2-harfbuzz-0.0 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/cache/apt/*

COPY requirements.txt /tmp
WORKDIR /tmp
USER airflow
RUN pip install -r requirements.txt