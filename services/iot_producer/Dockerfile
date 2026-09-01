FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY services/iot_producer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY services/__init__.py ./services/__init__.py
COPY services/iot_producer ./services/iot_producer
COPY data/iot ./data/iot

EXPOSE 8101
CMD ["uvicorn", "services.iot_producer.app:app", "--host", "0.0.0.0", "--port", "8101", "--no-access-log"]
