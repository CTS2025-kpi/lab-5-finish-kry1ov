#!/bin/bash
set -e

if [ -f "./.venv/bin/activate" ]; then
  . ./.venv/bin/activate
  uv sync
else
  pip install -q grpcio grpcio-tools
fi

echo "Generating gRPC code from database.proto..."
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. database.proto

if [ ! -f "database_pb2.py" ] || [ ! -f "database_pb2_grpc.py" ]; then
    echo "Error: Generated files not found."
    exit 1
fi

echo "Generated database_pb2.py and database_pb2_grpc.py."

mkdir -p coordinator
mkdir -p shard

echo "Copying generated files to coordinator/ ..."
cp -f database_pb2.py coordinator/
cp -f database_pb2_grpc.py coordinator/

echo "Copying generated files to shard/ ..."
cp -f database_pb2.py shard/
cp -f database_pb2_grpc.py shard/

echo "Moving generated files to test/ ..."
mv -f database_pb2.py test/
mv -f database_pb2_grpc.py test/

echo "Build script finished successfully."