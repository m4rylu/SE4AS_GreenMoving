FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY S1.py .
COPY config.ini .

# Comando per avviare lo script
CMD ["python", "S1.py"]