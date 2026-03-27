FROM python:3.11-slim

WORKDIR /app

# Install build tools needed by some Python packages (psycopg2-binary ships
# its own libpq so no extra system libs are required beyond gcc for any
# other native extensions that might be pulled in transitively)
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run as non-root for GCP best-practice security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# GCP Cloud Run injects PORT (default 8080); fallback keeps local docker run working
EXPOSE 8080

# Use gthread worker class: each gunicorn worker handles multiple concurrent
# requests via threads — critical for Cloud Run which routes many requests to
# a single container instance to minimise cold-starts.
CMD exec gunicorn app:app \
    --bind "0.0.0.0:${PORT:-8080}" \
    --workers 2 \
    --worker-class gthread \
    --threads 4 \
    --timeout 120 \
    --graceful-timeout 30 \
    --log-level info \
    --access-logfile -
