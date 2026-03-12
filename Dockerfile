FROM python:3.11-slim
WORKDIR /app
# Installiamo i driver per connetterci al database
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Teniamo il container acceso per dare i comandi
CMD ["tail", "-f", "/dev/null"]