sudo find / -name redis
sudo cd redis-dir
sudo vim redis.conf

# add in redis.conf:
maxmemory 16,777,216
maxmemory-policy allkeys-lru

python -m grpc_tools.protoc -I ./ --python_out=./ --grpc_python_out=./ Distribute.proto

mkdir -p ClientFiles
mkdir -p Database
mkdir -p DirectoryServerFiles/8010
mkdir -p DirectoryServerFiles/8011
mkdir -p DirectoryServerFiles/8012
