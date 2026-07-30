# AWS platform test infrastructure

One-time setup for running the Iceberg feature suite against managed AWS engines
(currently EMR Serverless) from GitHub Actions. See
`.github/workflows/aws-platform-tests.yml` for the job itself.

## 1. Create the storage

Create an S3 bucket for scripts, warehouse data, logs and reports, plus an
S3 Tables bucket for the s3tables mode. Both are referenced by the template.

## 2. Deploy the roles

Creates the GitHub OIDC provider, the CI role (assumed from Actions via OIDC,
scoped to the `aws-live` environment), the EMR Serverless job execution role,
and a monthly budget alarm.

```bash
aws cloudformation deploy \
  --stack-name icebergmatrix-ci \
  --template-file infra/aws/aws.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
      DataBucket=<your-bucket> \
      TableBucketArn=arn:aws:s3tables:<region>:<acct>:bucket/<name> \
      BudgetNotificationEmail=you@example.com

aws cloudformation describe-stacks --stack-name icebergmatrix-ci \
  --query 'Stacks[0].Outputs' --output table
```

If the deploy fails with `EntityAlreadyExists` on the OIDC provider, the account
already has one: find it with `aws iam list-open-id-connect-providers` and pass
it as `ExistingOidcProviderArn`.

## 3. Set the bucket lifecycle

Expires test data and logs after a day, keeps reports for 30, and aborts
incomplete multipart uploads. That last rule matters most: cancelled Spark jobs
leave upload parts that never show up in a listing but do bill.

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket iceberg-tests-matrix \
  --lifecycle-configuration file://infra/aws/s3-lifecycle.json
```

Note this does **not** cover the S3 Tables bucket — bucket lifecycle rules don't
apply to table buckets, and S3 Tables bills for its own automatic maintenance
even when idle. Cleanup there means deleting namespaces and tables, which
`tests/aws/teardown.py` does.

## 4. Service-linked role for EMR Serverless

The first `CreateApplication` in an account has to create the service-linked
role `AWSServiceRoleForAmazonEMRServerless`, otherwise the call fails with
`Access denied when calling CreateServiceLinkedRole`.

The template grants the CI role permission to create it (scoped to that one
service), so a stack deployed from step 2 handles this on its own. To create it
up front instead — or if the stack predates that grant — run once per account:

```bash
aws iam create-service-linked-role \
  --aws-service-name ops.emr-serverless.amazonaws.com
```

`InvalidInput ... has been taken in this account` means it already exists.

## 5. Configure the GitHub environment

Settings → Environments → `aws-live`. The name must match: the OIDC trust policy
is scoped to `repo:<owner>/<repo>:environment:aws-live`, so a token minted from
any other context cannot assume the role.

- Required reviewer: yourself. Anyone with write access can dispatch the
  workflow, but the job then waits for your approval before reaching AWS.
- Deployment branch policy: restrict to the branches you want to run from.
- Environment secrets (not repository secrets), from the stack outputs:

| Secret | Value |
|---|---|
| `AWS_CI_ROLE_ARN` | `CiRoleArn` output |
| `AWS_EMR_JOB_ROLE_ARN` | `EmrJobRoleArn` output |
| `AWS_DATA_BUCKET` | your S3 bucket name |
| `AWS_TABLE_BUCKET_ARN` | your S3 Tables bucket ARN |
| `AWS_REGION` | the region everything lives in |

## 6. Run it

Dispatch **AWS Platform Tests** with `dry-run: true` first. That assumes the
role and stops, proving the OIDC trust and the environment gate without
provisioning anything billable. A successful run prints an
`assumed-role/icebergmatrix-ci/<run_id>` identity.

Then run `modes: s3buckets` on its own before `both`, so the first real run has
one variable. Teardown runs under `always()` and prints either
`all billable resources removed` or a `COULD NOT CLEAN` list naming what to
check by hand.

## 7. How the s3tables mode reaches S3 Tables

Both modes talk to the AWS Glue Data Catalog. They differ in which catalog and
where the data lands:

| | `s3buckets` | `s3tables` |
|---|---|---|
| catalog-impl | `GlueCatalog` | `GlueCatalog` |
| catalog | default Glue catalog | federated `s3tablescatalog` via `glue.id` |
| table data | the `warehouse` path | service-managed by S3 Tables |
| `DROP TABLE` | plain | must say `PURGE` |

The s3tables configuration is:

```
--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.local.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
--conf spark.sql.catalog.local.glue.id=<account>:s3tablescatalog/<table-bucket>
--conf spark.sql.catalog.local.client.region=<region>
--conf spark.sql.catalog.local.warehouse=s3://<bucket>/emr/warehouse/<run>/
```

Prerequisite: the table bucket must be integrated with AWS analytics services,
which creates the `s3tablescatalog` federated catalog. Check with
`aws glue get-catalog --catalog-id s3tablescatalog`. Ours is in IAM mode
(`CreateTableDefaultPermissions` granting `ALL` to `IAM_ALLOWED_PRINCIPALS`), so
no Lake Formation grants are needed.

### Four things that are not obvious

**The warehouse is required but never used.** S3 Tables assigns its own
service-managed location, yet `GlueCatalog` derives a table location on the
client *before* calling Glue and rejects an empty warehouse with
`Cannot derive default warehouse location, warehouse path must not be null or
empty`. So a real, permitted S3 path has to be configured. Nothing is ever
written there -- verified: the probe created a table and the path stayed empty
while the table appeared under `aws s3tables list-tables`.

**`DROP TABLE` must say `PURGE`.** S3 Tables owns the storage and refuses a
metadata-only drop: `S3 managed Iceberg table must be purged when dropped`. The
suite handles this through `_drop_table()`, gated on `ICEBERG_DROP_PURGE` so that
other platforms keep a plain drop rather than silently deleting data.

**`type=rest` does not work on this image.** The AWS Glue Iceberg REST endpoint
configuration (`type=rest`, `uri=https://glue.<region>.amazonaws.com/iceberg`,
sigv4) looks like the natural fit, and namespace listing even succeeds, but the
EMR 8 Iceberg build ignores it and constructs a `GlueCatalog` anyway. Unwrapping
the catalog with `tests/aws/s3tables_probe.py` shows
`SparkCatalog -> CachingCatalog -> GlueCatalog` regardless. Table creation then
fails in `defaultWarehouseLocation`. Confirmed under two different catalog names,
so it is not a name collision.

**The native client needs a VPC.** `software.amazon.s3tables.iceberg.S3TablesCatalog`
calls `s3tables.<region>.amazonaws.com` directly, and an EMR Serverless
application with no `networkConfiguration` has no route there -- every call dies
with `Connect timed out` after about 57 seconds, which makes a whole suite take
an hour and produce nothing. S3 Tables is not among the services EMR Serverless
reaches by default. Using it would mean a VPC plus a PrivateLink interface
endpoint, and then also endpoints for S3, Glue, STS and CloudWatch Logs, since
entering a VPC forfeits the default managed access. The Glue federation above
avoids all of that.

### IAM

Federated catalogs sit in their own ARN hierarchy, with the catalog and bucket as
extra leading segments, so they need policy entries of their own:

```
catalog/s3tablescatalog
catalog/s3tablescatalog/<bucket>
database/s3tablescatalog/<bucket>/<prefix>*
table/s3tablescatalog/<bucket>/<prefix>*/*
userDefinedFunction/s3tablescatalog/<bucket>/<prefix>*/*
```

Three separate permission failures were needed to find all of that, so if a run
fails with `AccessDeniedException`, read the resource ARN in the message rather
than assuming which entry is missing:

- `glue:GetCatalog` is needed on the **root** `:catalog` as well, not just the
  federated one: resolving a federated catalog walks the hierarchy from the top.
- `glue:DeleteDatabase` is authorised against `userDefinedFunction` too, under
  the federated path, or `DROP NAMESPACE` fails.
- `lakeformation:GetDataAccess` is needed because credential vending for
  federated tables goes through Lake Formation even in IAM mode.

### Diagnosing catalog problems

`tests/aws/s3tables_probe.py` reports the effective configuration, unwraps the
catalog chain to the real implementation class, and walks
create/insert/select/drop. Reading the configuration alone is misleading, because
the configuration can be exactly right while the resolved class is something
else entirely. Note that the probe must use a namespace carrying the resource
prefix; any other name fails on IAM and hides the real result.
