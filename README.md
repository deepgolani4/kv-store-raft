
```bash
setup:
make
./coordinator 127.0.0.1 9000
./raft_node 127.0.0.1 9101 g1 127.0.0.1:9102 127.0.0.1:9103
./raft_node 127.0.0.1 9102 g1 127.0.0.1:9101 127.0.0.1:9103
./raft_node 127.0.0.1 9103 g1 127.0.0.1:9101 127.0.0.1:9102
./client 127.0.0.1 9200
```

## Benchmarks

clone ycsb repo and run
```bash
./ycsb/setup.sh /tmp/ycsb-official
```

Run benchmarks
load:
```bash
cd /tmp/ycsb-official
./bin/ycsb load kvraft -P workloads/workloada \
  -p kvraft.host=127.0.0.1 -p kvraft.port=9000
```

run with integrity checks:
```bash
./bin/ycsb run kvraft -P workloads/workloada \
  -p kvraft.host=127.0.0.1 -p kvraft.port=9000 \
  -p dataintegrity=true -p fieldcount=1
```

Leader failure

run YCSB while killing leader after 1 second:
```bash
cd /tmp/ycsb-official
(
  sleep 1
  LEADER=$(printf 'leader_sync\n' | nc -w 2 127.0.0.1 9000 | cut -d'|' -f2)
  LPORT=${LEADER##*:}
  LPID=$(lsof -t -iTCP:${LPORT} -sTCP:LISTEN | head -n 1)
  kill -9 "${LPID}"
) &
./bin/ycsb run kvraft -s -P workloads/workloada \
  -p kvraft.host=127.0.0.1 -p kvraft.port=9000 \
  -p recordcount=2000 -p operationcount=10000 -p threadcount=8 \
  -p dataintegrity=true -p fieldcount=1 \
  > /tmp/kvraft-ycsb-leader-crash.out 2>&1
```

Check throughput + accuracy counters:
```bash
rg -n "\[OVERALL\]|\[READ\], Return=|\[UPDATE\], Return=|\[VERIFY\], Return=|Return=ERROR|UNEXPECTED|FAILED" \
  /tmp/kvraft-ycsb-leader-crash.out
```


## Files on Disk
Each node keeps:
- `wal_<port>.log`
- On restart, a node asks coordinator for leader and pulls full KV from leader.

