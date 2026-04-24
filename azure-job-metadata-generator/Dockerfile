# Metadata Generator — slim container that produces the semantic metadata
# layer for native dashboard filtering.
#
# Runs as a Container Apps Job execution per generation request.
#
# Build:  docker build -t metadata-generator -f Dockerfile .
# Push:   docker push <acr>.azurecr.io/metadata-generator:latest

FROM python:3.11-slim

WORKDIR /app

# DuckDB needs CA certs for the Azure extension over HTTPS.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY *.py /app/

ENTRYPOINT ["python", "-m", "main"]
