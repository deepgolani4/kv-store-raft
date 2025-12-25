#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
YCSB_DIR="${1:-/tmp/ycsb-official}"

mkdir -p "${YCSB_DIR}/kvraft"
rm -rf "${YCSB_DIR}/kvraft"
cp -R "${REPO_ROOT}/ycsb/kvraft" "${YCSB_DIR}/kvraft"

if ! rg -q '<module>kvraft</module>' "${YCSB_DIR}/pom.xml"; then
  perl -0777 -i -pe 's#(<module>kudu</module>\n)#$1    <module>kvraft</module>\n#' "${YCSB_DIR}/pom.xml"
fi

if ! rg -q '"kvraft"\s*:\s*"site\.ycsb\.db\.KvRaftClient"' "${YCSB_DIR}/bin/ycsb"; then
  perl -0777 -i -pe 's#("kudu"\s*:\s*"site\.ycsb\.db\.KuduYCSBClient",\n)#$1    "kvraft"       : "site.ycsb.db.KvRaftClient",\n#' "${YCSB_DIR}/bin/ycsb"
fi

echo "ok: kvraft binding installed into ${YCSB_DIR}"
echo "next:"
echo "  cd ${YCSB_DIR}"
echo "  ./bin/ycsb load kvraft -P workloads/workloada -p kvraft.host=127.0.0.1 -p kvraft.port=9000"
echo "  ./bin/ycsb run  kvraft -P workloads/workloada -p kvraft.host=127.0.0.1 -p kvraft.port=9000 -p dataintegrity=true -p fieldcount=1"
