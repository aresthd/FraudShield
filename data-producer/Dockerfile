FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
COPY python-producer.py .
COPY helpers.py .
COPY config/static-data.yml config/static-data.yml
COPY config/fraud-rules.json config/fraud-rules.json

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "python-producer.py"]