## Distributed File System

This is a Distributed File System, implemented with Python, Redis, process comm framework: gRPC/bare TCP socket, sqlite3, supporting PAXOS consistency check, Traffic Management, concurrency control in async processes of different servers.



installing python requirement environments in Windows CMD:

```shell
pip3 install -r requirements.txt
wsl
```

Configuring Running environment: Redis max-memory usage

```shell
sudo find / -name redis
sudo cd redis-dir
sudo vim redis.conf
```

add two lines into redis.conf file:

```
maxmemory 16,777,216
maxmemory-policy allkeys-lru
```

Generating gRPC files:

```shell
python -m grpc_tools.protoc -I ./ --python_out=./ --grpc_python_out=./ Distribute.proto
```

Into Windows-Sub-Linux Terminal, running Redis, Client/Server:

```shell
wsl
redis-server
mkdir -p ClientFiles
mkdir -p Database
mkdir -p DirectoryServerFiles/8010
mkdir -p DirectoryServerFiles/8011
mkdir -p DirectoryServerFiles/8012

bash run.sh
```

