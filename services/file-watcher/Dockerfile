FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY file_watcher.py .
COPY config.yaml .

CMD ["python", "file_watcher.py", "--config", "config.yaml"]