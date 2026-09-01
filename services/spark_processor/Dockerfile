FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
COPY services/spark_processor/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --retries 10 --timeout 120 -r requirements.txt
COPY services/spark_processor/app.py ./app.py

CMD ["python", "app.py"]
