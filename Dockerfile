FROM python:3.12-slim

WORKDIR /app

# Copiar requirements
COPY requirements.txt .

# Install dependencies (sem cache)
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY intelligent_daemon_lite.py .
COPY technical_analysis.py .
COPY market_data.py .

# Run
CMD ["python", "intelligent_daemon_lite.py"]
