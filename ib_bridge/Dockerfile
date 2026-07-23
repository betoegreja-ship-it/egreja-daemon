FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir ib_insync==0.9.86 flask==3.0.3
COPY bridge.py .
CMD ["python", "bridge.py"]
