# Distributed Key-Value Store




## Build
```bash
make
```

## Run


```bash
./coordinator 127.0.0.1 9000
```

```bash
./raft_node 127.0.0.1 9101 g1 127.0.0.1:9102 127.0.0.1:9103
```

```bash
./raft_node 127.0.0.1 9102 g1 127.0.0.1:9101 127.0.0.1:9103
```

```bash
./raft_node 127.0.0.1 9103 g1 127.0.0.1:9101 127.0.0.1:9102
```

```bash
./client 127.0.0.1 9200
```

## Example
```text
>> put:user42:alice
put_success
>> get:user42
alice
>> update:user42:alice_v2
update_success
>> get:user42
alice_v2
>> delete:user42
delete_success
>> get:user42
key_error
```

Direct write to a follower:
```bash
printf '{"role":"put","key":"k1","value":"v1"}' | nc 127.0.0.1 9102
```

Verify replcation on all nodes
```bash
for p in 9101 9102 9103; do
  echo "node:$p"
  printf '{"role":"get","key":"k1"}' | nc 127.0.0.1 $p | tr -d '\000'
  echo
done
```

## Client Commands
- `put:<key>:<value>`
- `get:<key>`
- `update:<key>:<value>`
- `delete:<key>`
- `exit`


