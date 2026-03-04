FROM python:3.11-slim

# Instalar Node.js
RUN apt-get update && apt-get install -y curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean

WORKDIR /app

# Instalar dependências Python
COPY requirements.txt .
RUN pip install -r requirements.txt

# Instalar dependências Node.js
COPY package*.json ./
RUN npm install --legacy-peer-deps
# Copiar código
COPY . .

EXPOSE 3001

CMD ["bash", "start-all.sh"]
