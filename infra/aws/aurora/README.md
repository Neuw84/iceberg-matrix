# Aurora PostgreSQL platform test infrastructure

The `aws-aurora` matrix row is measured against Amazon Aurora PostgreSQL's
`aurora_analytics` feature (DuckDB embedded in Aurora, reading Iceberg/Parquet
as read-only foreign tables). Unlike the EMR/Glue/Redshift suites, Aurora needs
a **standing cluster** reached through an **SSM bastion** — the cluster is
private, with no public access and no inbound security-group rule from the
internet.

`aurora.yaml` provisions everything the suite needs in one stack:

- the `AuroraAnalytics` feature IAM role (Glue + S3 read, `lakeformation:GetDataAccess`);
- a DB cluster parameter group with `aurora_analytics.enabled=true` (dynamic, no reboot);
- a private Aurora PostgreSQL cluster + single writer, master password in Secrets Manager;
- Glue and STS **interface** VPC endpoints with their security group, so the
  private cluster can reach those APIs with no NAT;
- an SSM-managed bastion (Amazon Linux 2023, `psql` + `jq` installed, no key pair,
  no inbound rules) that the suite runs SQL through.

## Why the VPC endpoints matter

The analytics engine runs *inside* the cluster, so it needs network egress to
Glue (catalog resolution), STS (assuming the AuroraAnalytics role) and S3 (data
files). A private cluster with no NAT reaches none of these by default — every
Glue/STS call dies with `curlCode: 28, Connection timed out` after ~2s, which
looks like a permissions problem but is not.

The endpoint security group needs **both** a self-reference from the Aurora SG
*and* a VPC-CIDR rule on 443. A plain SG self-reference is not enough: the
cluster's **same-AZ** traffic to the interface-endpoint ENI is only admitted by
the CIDR rule (measured — same-AZ hairpin was blocked with SG-reference alone).
`VpcCidr` defaults to `172.31.0.0/16` (the default VPC).

## Prerequisite not in the stack: the S3 gateway endpoint

S3 reads go through an **S3 gateway** VPC endpoint, which attaches to a route
table rather than to subnets. The default VPC already has one, and a second
gateway endpoint for the same service in the same route table conflicts, so the
template does **not** create it. Confirm one exists and is associated with the
route table your cluster subnets use:

```bash
aws ec2 describe-vpc-endpoints \
  --filters Name=service-name,Values=com.amazonaws.<region>.s3 Name=vpc-id,Values=<vpc> \
  --query 'VpcEndpoints[].{id:VpcEndpointId,rts:RouteTableIds}'
```

If there is none, create it and associate it with that route table before running.

## Deploy

```bash
aws cloudformation deploy \
  --stack-name icebergmatrix-aurora \
  --template-file infra/aws/aurora/aurora.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
      VpcId=<vpc-id> \
      'SubnetIds=<subnet-a>,<subnet-b>,<subnet-c>' \
      BastionSubnetId=<subnet-a> \
      EngineVersion=17.11 \
      ParameterGroupFamily=aurora-postgresql17
```

Cluster creation takes ~10–15 min. Outputs: `ClusterEndpoint`, `MasterSecretArn`,
`BastionInstanceId`, `AnalyticsRoleArn`.

## Run the suite locally (through the bastion)

The suite drives the bastion with `ssm:SendCommand` (boto3) — no local psql,
tunnel, or session-manager-plugin needed. The bastion fetches the managed master
password itself.

```bash
export AWS_REGION=us-east-1
export AURORA_BASTION=<BastionInstanceId>
export AURORA_ENDPOINT=<ClusterEndpoint>
export AURORA_SECRET=<MasterSecretArn>
export AURORA_GLUE_ACCOUNT=<account-id>
export AURORA_VERSION=17.11
uv run --with boto3 python tests/aurora_feature_tests.py
```

The V2 Iceberg fixtures (position deletes, hidden partitioning, partition
evolution) are built with Athena engine v3:

```bash
export AURORA_FIXTURE_DB=aurora_probe_db
export AURORA_FIXTURE_LOCATION=s3://<bucket>/aurora-probe-fixtures
export AURORA_ATHENA_WORKGROUP=<engine-v3-workgroup>
export AURORA_ATHENA_OUTPUT=s3://<results-bucket>/aurora-probe/
uv run --with boto3 python tests/aws/aurora_fixtures.py create   # ... drop when done
```

V3 fixtures (deletion vectors, nanosecond timestamps, geometry, lineage) are
**not** built: Athena v3 rejects `format_version=3` and pyiceberg 0.11.1 cannot
write V3. Those cells stay honest `skip`s in the report; producing them would
need Spark + Iceberg 1.11 writing into Glue (future work). The embedded DuckDB
build does reject the one V3 type that was reachable (VARIANT), which is the
measured basis for rating the V3-type cells `none`.

## CI

`.github/workflows/aurora-tests.yml` provisions this stack, builds the Athena
fixtures, runs the suite via the bastion, and tears the stack down under
`always()`. Dispatch-only, gated by the `aws-live` environment. Besides the
standard `AWS_CI_ROLE_ARN` / `AWS_REGION` / `AWS_DATA_BUCKET` secrets it needs:

| Secret | Value |
|---|---|
| `AURORA_VPC_ID` | VPC to place the cluster and bastion in |
| `AURORA_SUBNET_IDS` | comma-separated subnets (≥2 AZs) for the DB subnet group |
| `AURORA_BASTION_SUBNET` | one subnet for the bastion |
| `AWS_ACCOUNT_ID` | account id, used to build Glue ARNs |
| `AURORA_ATHENA_WORKGROUP` | an Athena engine-v3 workgroup for fixtures |
| `AURORA_BASIC_TABLE` | existing Iceberg table `db/table` for basic reads |
| `AURORA_VARIANT_TABLE` | existing V3 VARIANT table `db/table` |

The CI role also needs the Aurora provisioning + PassRole grants described in
the top-level `infra/aws/README.MD`.

## Teardown

```bash
aws cloudformation delete-stack --stack-name icebergmatrix-aurora
```

The parameter group and VPC endpoints are part of the stack and go with it. The
S3 gateway endpoint (a prerequisite, not stack-owned) is left alone.
