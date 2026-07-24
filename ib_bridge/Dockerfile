FROM python:3.11-slim
WORKDIR /app
# tzdata: ib_insync parseia horario dos fills com US/Eastern (execDetails).
# Sem os dados IANA -> ZoneInfoNotFoundError quebra o handler de fill.
RUN pip install --no-cache-dir ib_insync==0.9.86 flask==3.0.3 tzdata==2024.1
COPY bridge.py .
CMD ["python", "bridge.py"]
