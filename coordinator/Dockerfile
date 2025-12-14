FROM python:3.10-slim

WORKDIR /app

COPY coordinator/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY coordinator/database_pb2.py .
COPY coordinator/database_pb2_grpc.py .
COPY coordinator/coordinator.py .

EXPOSE 50050

CMD ["python", "coordinator.py"]