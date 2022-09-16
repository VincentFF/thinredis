# thinRedis

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/VincentFF/thinredis/blob/main/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/VincentFF/thinredis)](https://goreportcard.com/report/github.com/VincentFF/thinredis)  
**thinRedis** is a high performance standalone cache server written by GO.  
It implemented full [RESP](https://redis.io/docs/reference/protocol-spec/)(Redis Serialization Protocol), so it supports
all Redis clients.

## Features

* Support all Clients based on RESP protocol
* Support String, List, Set, Hash data types
* Support TTL(Key-Value pair will be deleted after TTL)
* Full in-memory storage
* Support atomic operation for some needed commands(like INCR, DECR, INCRBY, MSET, SMOVE, etc.)

## Usage
Build thinRedis from source code:
```bash
$ go build -o thinRedis main.go
```
Start thinRedis server:
```bash
$ ./thinRedis
[info][server.go:26] 2022/09/08 13:23:50 [Server Listen at  127.0.0.1 : 6379]
```
Use start option commands or config file to change default settings:
```bash 
$ ./thinRedis -h
Usage of ./thinredis:
  -config string
        Appoint a config file: such as /etc/redis.conf
  -host string
        Bind host ip: default is 127.0.0.1 (default "127.0.0.1")
  -logdir string
        Set log directory: default is /tmp (default "./")
  -loglevel string
        Set log level: default is info (default "info")
  -port int
        Bind a listening port: default is 6379 (default 6379)
```
## Communication with thinRedis server
Any redis client can communicate with thinRedis server.  
For example, use redis-cli to communicate with thinRedis server:

```bash
# start a thinRedis server listening at 12345 port
$ ./thinredis -port 12345
[info][server.go:26] 2022/09/08 13:31:47 [Server Listen at  127.0.0.1 : 12345]
                      ...

# start a redis-cli and connect to thinRedis server
$ redis-cli -p 12345
127.0.0.1:12345> PING
PONG
127.0.0.1:12345> MSET key1 a key2 b
OK
127.0.0.1:12345> MGET key1 key2 nonekey
1) "a"
2) "b"
3) (nil)
127.0.0.1:12345> RPUSH list1 1 2 3 4 5
(integer) 5
127.0.0.1:12345> LRANGE list1 0 -1
1) "1"
2) "2"
3) "3"
4) "4"
5) "5"
127.0.0.1:12345> TYPE list1
list
127.0.0.1:12345> EXPIRE list1 100
(integer) 1
# wait for a few seconds
127.0.0.1:12345> TTL list1
(integer) 93
127.0.0.1:12345> PERSIST list1
(integer) 1
127.0.0.1:12345> TTL list1
(integer) -1
```


## Benchmark

Benchmark result is based on [redis-benchmark](https://redis.io/topics/benchmarks) tool.  
Testing on ThinkBook Laptop with AMD Ryzen 7 5800H@3.20GHz, 16.0 GB RAM, and on windows 11 wsl2 ubuntu 22.04 system.

`benchmark -c 50 -n 200000`

```text
get: 168634.06 requests per second
set: 167644.59 requests per second
incr: 164068.91 requests per second
lpush: 165152.77 requests per second
rpush: 162601.62 requests per second
lpop: 165152.77 requests per second
rpop: 165562.92 requests per second
sadd: 161420.50 requests per second
hset: 162469.55 requests per second
spop: 168350.17 requests per second

lrange_100: 50620.10 requests per second
lrange_300: 20132.88 requests per second
lrange_500: 12051.10 requests per second
lrange_600: 11992.56 requests per second
mset: 109289.62 requests per second
```

## Support Commands
All commands used as [redis commands](https://redis.io/commands/). You can use any redis client to communicate with thinRedis.

| key     | string      | list   | set         | hash         |
|---------|-------------|--------|-------------|--------------|
| del     | set         | llen   | sadd        | hdel         |
| exists  | get         | lindex | scard       | hexists      |
| keys    | getrange    | lpos   | sdiff       | hget         |
| expire  | setrange    | lpop   | sdirrstore  | hgetall      |
| persist | mget        | rpop   | sinter      | hincrby      |
| ttl     | mset        | lpush  | sinterstore | hincrbyfloat |
| type    | setex       | lpushx | sismember   | hkeys        |
| rename  | setnx       | rpush  | smembers    | hlen         |
|         | strlen      | rpushx | smove       | hmget        |
|      | incr        | lset   | spop        | hset         |
|      | incrby      | lrem   | srandmember | hsetnx       |
|      | decr        | ltrim  | srem        | hvals        |
|      | decrby      | lrange | sunion      | hstrlen      |
|      | incrbyfloat | lmove  | sunionstore | hrandfield   |
|      | append      |        |             |              |
