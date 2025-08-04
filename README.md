# CLI test
## 1. Run etcd server: 1 tab
./etcd

## 2. Run cedar-agent with etcd watcher: 1 tab
cargo run


## 3. Put policy: 1 tab
etcdctl put cedar/policy/allow_alice 'permit(principal == User::"alice", action, resource);'

## 4. Update entities
etcdctl put cedar/data/entities '[{"uid":{"type":"User","id":"alice"},"attrs":{},"parents":[]},{"uid":{"type":"Document","id":"doc1"},"attrs":{"owner":"alice"},"parents":[]}]'

## 5. Check authorization (should return "allowed": true)
curl -X POST -H "Content-Type: application/json" -d '{"principal":"User::\"alice\"","action":"Action::\"read\"","resource":"Document::\"doc1\""}' http://localhost:8180/v1/is_authorized
