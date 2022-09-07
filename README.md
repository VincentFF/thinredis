# thinRedis
[![Go Report Card](https://goreportcard.com/badge/github.com/VincentFF/thinredis)](https://goreportcard.com/report/github.com/VincentFF/thinredis)  
 **thinRedis** is a high performance standalone cache server written by GO.  
 It implemented full [RESP](https://redis.io/docs/reference/protocol-spec/)(Redis Serialization Protocol), so it supports all Redis clients.  

## Features
* Support all Clients based on RESP protocol
* Support String, List, Set, Hash data types
* Support TTL(Key-Value pair will be deleted after TTL)
* Full memory storage
* Support atomic operation for some needed commands(like INCR, DECR, INCRBY, MSET, SMOVE, etc.)

## Benchmark 
