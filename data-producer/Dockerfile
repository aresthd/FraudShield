FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
COPY main.py .
COPY utils/ ./utils/
COPY config/static-data.yml config/static-data.yml

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]