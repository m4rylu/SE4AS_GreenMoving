FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bike_operator.py .

# Comando per avviare lo script
CMD ["python", "bike_operator.py"]