#!/bin/bash
# Run a SQL file on the Aurora cluster THROUGH the SSM bastion (no public access,
# no local psql/tunnel needed). Usage:
#   AWS_REGION=us-east-1 AWS_PROFILE=duckdemo \
#   tests/aws/aurora_ssm_probe.sh <sql-file> [database]
#
# The bastion fetches the master password from Secrets Manager itself. Prints the
# psql stdout/stderr the command produced.
set -uo pipefail

SQL_FILE="${1:?usage: aurora_ssm_probe.sh <sql-file> [database]}"
DB="${2:-postgres}"

BID="${AURORA_BASTION:-i-0cbd028df6b73f2a9}"
EP="${AURORA_ENDPOINT:-icebergmatrix-aurora-cluster.cluster-cqugqrwixelf.us-east-1.rds.amazonaws.com}"
SECRET="${AURORA_SECRET:-arn:aws:secretsmanager:us-east-1:378683551918:secret:rds!cluster-1dfa7192-c6b3-467c-87a0-765f011fa2fe-kc7spq}"
REGION="${AWS_REGION:-us-east-1}"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# The remote script: base64-decode the SQL, fetch creds, run psql.
SQL_B64=$(base64 < "$SQL_FILE" | tr -d '\n')
cat > "$WORK/remote.sh" <<REMOTE
#!/bin/bash
set -uo pipefail
echo "$SQL_B64" | base64 -d > /tmp/probe.sql
SJSON=\$(aws secretsmanager get-secret-value --secret-id "$SECRET" --query SecretString --output text --region "$REGION")
export PGPASSWORD=\$(echo "\$SJSON" | jq -r .password)
PGUSER=\$(echo "\$SJSON" | jq -r .username)
psql -h "$EP" -p 5432 -d "$DB" -U "\$PGUSER" -v ON_ERROR_STOP=0 -f /tmp/probe.sql 2>&1
rm -f /tmp/probe.sql
REMOTE

python3 - "$WORK/remote.sh" "$WORK/params.json" <<'PY'
import json, sys
script = open(sys.argv[1]).read()
json.dump({"commands": [script]}, open(sys.argv[2], "w"))
PY

CID=$(aws ssm send-command --instance-ids "$BID" \
  --document-name "AWS-RunShellScript" \
  --cli-input-json "{\"Parameters\": $(cat "$WORK/params.json")}" \
  --query 'Command.CommandId' --output text --region "$REGION" 2>&1)
if [ -z "$CID" ] || echo "$CID" | grep -qi error; then
  echo "send-command failed: $CID" >&2
  exit 1
fi

for _ in $(seq 1 40); do
  sleep 5
  ST=$(aws ssm get-command-invocation --command-id "$CID" --instance-id "$BID" \
       --query Status --output text --region "$REGION" 2>/dev/null)
  case "$ST" in
    Success|Failed|Cancelled|TimedOut) break ;;
  esac
done

aws ssm get-command-invocation --command-id "$CID" --instance-id "$BID" \
  --query 'StandardOutputContent' --output text --region "$REGION" 2>&1
ERR=$(aws ssm get-command-invocation --command-id "$CID" --instance-id "$BID" \
  --query 'StandardErrorContent' --output text --region "$REGION" 2>&1)
[ -n "$ERR" ] && { echo "----- STDERR -----"; echo "$ERR"; }
echo "----- SSM status: ${ST:-unknown} -----"
