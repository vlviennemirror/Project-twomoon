FROM node:20-alpine AS frontend

WORKDIR /build

COPY web_hub/frontend/package.json web_hub/frontend/package-lock.json* ./
RUN npm ci

COPY web_hub/frontend/ ./
RUN npm run build


FROM python:3.11-slim AS production

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared_lib/ ./shared_lib/
COPY web_hub/ ./web_hub/

COPY --from=frontend /build/dist ./web_hub/frontend/dist

RUN useradd --create-home --shell /bin/bash twomoon
RUN mkdir -p /home/twomoon/.postgresql && \
    curl -o /home/twomoon/.postgresql/root.crt 'https://cockroachlabs.cloud/clusters/routing/v1/defaultdb/cert' && \
    chown -R twomoon:twomoon /home/twomoon/.postgresql
USER twomoon

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "web_hub.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--loop", "uvloop"]
