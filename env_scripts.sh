sudo find / -name redis

sudo cd redis-dir
sudo vim redis.conf

add:

maxmemory 16,777,216
maxmemory-policy allkeys-lru
