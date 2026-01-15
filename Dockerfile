FROM python:3.11-slim

WORKDIR /app

# Install curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY templates/ ./templates/

# Create temp directory for zone files
RUN mkdir -p /app/temp

# Build version - change to force rebuild
ENV BUILD_VERSION="2026-01-16-v2"

# Environment variables (defaults)
ENV ICANN_USER=""
ENV ICANN_PASS=""
ENV DB_HOST="clickhouse-db"
ENV CLICKHOUSE_PASSWORD=""
ENV PORT=8080

# Parallel processing defaults (optimize for 12 cores, 64GB RAM)
ENV PARALLEL_ENABLED="true"
ENV DOWNLOAD_WORKERS="4"
ENV PARSE_WORKERS="8"
ENV PARALLEL_CHUNK_SIZE="100000"
ENV BATCH_SIZE="200000"
ENV CHUNK_SIZE="200000"
ENV LARGE_FILE_THRESHOLD="5000000"
ENV GC_INTERVAL="10"

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Run application
CMD ["python", "-m", "src.main"]
