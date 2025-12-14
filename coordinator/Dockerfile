FROM python:3.10-slim

WORKDIR /app

COPY coordinator/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y curl && \
    curl -SL https://github.com/docker/compose/releases/download/v2.29.0/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose && \
    chmod +x /usr/local/bin/docker-compose

COPY coordinator/database_pb2.py .
COPY coordinator/database_pb2_grpc.py .
COPY coordinator/coordinator.py .

EXPOSE 50050

CMD ["python", "coordinator.py"]