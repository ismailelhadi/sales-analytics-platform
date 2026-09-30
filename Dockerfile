FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
# Build everything on first start, then serve the assistant.
CMD ["bash", "-c", "./run_all.sh && uvicorn rag.api:app --host 0.0.0.0 --port 8000"]
