# Deploy on any container host (Render / Railway / Fly free tiers)
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Render/Railway/Fly set $PORT; default 8000 for local runs
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
