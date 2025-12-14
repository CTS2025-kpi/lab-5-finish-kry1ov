FROM python:3.10-slim

WORKDIR /app

COPY shard/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shard/database_pb2.py .
COPY shard/database_pb2_grpc.py .
COPY shard/shard.py .

EXPOSE 50051

CMD ["python", "shard.py"]