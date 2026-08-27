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

There is a second environment, `databricks-live`, which the Databricks workflow
uses so its credentials are not readable by the AWS jobs; the CI role trusts both
subjects. See section 9 for what goes on it.

- Required reviewer: yourself. Anyone with write access can dispatch the
  workflow, but the job then waits for your approval before reaching AWS.
- Deployment branch policy: restrict to the branches you want to run from.
- Environment secrets (not repository secrets), from the stack outputs:

| Secret | Value |
|---|---|
| `AWS_CI_ROLE_ARN` | `CiRoleArn` output |
| `AWS_EMR_JOB_ROLE_ARN` | `EmrJobRoleArn` output |
| `AWS_REDSHIFT_ROLE_ARN` | `RedshiftRoleArn` output (needed for `engine: redshift`) |
| `AWS_DATA_BUCKET` | your S3 bucket name |
| `AWS_TABLE_BUCKET_ARN` | your S3 Tables bucket ARN |
| `AWS_REGION` | the region everything lives in |

There is deliberately no secret for the Redshift admin password. The namespace is
created with `ManageAdminPassword`, so Secrets Manager owns the credential and the
driver looks the ARN up at run time with `redshift-serverless get-namespace`.
CloudFormation cannot return it: `AWS::RedshiftServerless::Namespace` rejects
`!GetAtt Namespace.AdminPasswordSecretArn` as "must be a readonly property in
schema", which is why the stack exposes `RedshiftAdminSecretHint` (the CLI command
to look it up) rather than the value.

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

## 8. How the Redshift engine differs

`engine: redshift` runs `tests/redshift_feature_tests.py` through the Redshift Data
API. There is no cluster-side bundle: the suite executes in the runner and every
statement is a `redshift-data` call, which is why `tests/aws/run_redshift.py` is so
much shorter than the EMR and Glue drivers.

### Four things that cost real time to discover

- **The auto-mounted catalog cannot write.** `CREATE TABLE awsdatacatalog.<db>.<t>
  ... USING ICEBERG` fails with `No session credential found`. Data access through
  `awsdatacatalog` is authorised with the caller's IAM session, and a Data API
  connection authenticated as a database user has none. Writes need an external
  schema that names the role:

  ```sql
  CREATE EXTERNAL SCHEMA s FROM DATA CATALOG DATABASE 'glue_db'
    IAM_ROLE 'arn:aws:iam::<acct>:role/icebergmatrix-redshift';
  CREATE TABLE s.t (id BIGINT, name VARCHAR) USING ICEBERG
    LOCATION 's3://<bucket>/<prefix>/';
  ```

- **S3 Tables needs a Glue resource link first.** A table bucket is a *federated*
  Glue catalog and Redshift cannot name one directly: both
  `"<bucket>@s3tablescatalog".ns.t` and putting the federated path in `CATALOG_ID`
  fail (the latter silently resolves against the default catalog, so the schema is
  created and every `SELECT` then raises `EntityNotFoundException`). The working
  route is a resource link in the **default** catalog whose `TargetDatabase` points
  at the federated namespace, named by an external schema with `CATALOG_ID` set to
  the **plain account id**:

  ```bash
  aws glue create-database --cli-input-json '{
    "CatalogId": "<acct>",
    "DatabaseInput": {"Name": "link_name", "TargetDatabase": {
      "CatalogId": "<acct>:s3tablescatalog/<bucket>", "DatabaseName": "<namespace>"}}}'
  ```
  ```sql
  CREATE EXTERNAL SCHEMA s FROM DATA CATALOG DATABASE 'link_name'
    IAM_ROLE '<role>' REGION '<region>' CATALOG_ID '<acct>';
  ```

  With that in place Redshift creates, reads, updates and deletes Iceberg tables in
  S3 Tables, omitting `LOCATION` because the table bucket owns placement.

- **`DROP TABLE` leaks data in the s3buckets mode.** It removes the Glue entry and
  leaves the Parquet and metadata behind, so the suite deletes its own
  `redshift/<run>/` prefix on the way out. In the s3tables mode `DROP TABLE` really
  does remove the table, so the two modes need different teardown.

- **Some features need a table Redshift cannot create.** It refuses
  `format-version 3` outright, so `tests/aws/redshift_fixtures.py` builds v3 tables,
  deletion vectors, a variant column and a branch/tag with Spark on EMR, and the
  suite then tries to read and write them. That is what separates "cannot write" from
  "cannot read": Redshift turns out to read v3 tables and apply deletion vectors
  correctly while refusing every v3 write, which is partial support rather than none.
  Spark 4 on `emr-spark-8.0.0` rejects `GEOMETRY` and `TIMESTAMP_NS` itself, so those
  two cells stay unmeasured rather than being guessed.

### Syntax notes

- `VARCHAR(N)` is rejected in an Iceberg table ("Use VARCHAR for strings"); use bare
  `VARCHAR`.
- The only writable table properties are `format-version` and `compression_type`.
  Anything else, including `write.delete.mode` and the bloom-filter properties, is
  refused with `... cannot be used in the PROPERTIES clause of "iceberg" table`.
- Defaults from `SHOW TABLE` differ per mode: `compression_type` is `snappy` on S3
  buckets and `zstd` on S3 Tables.
- On S3 Tables, `PARTITIONED BY` on `CREATE TABLE` is accepted and then silently
  discarded; `ALTER TABLE ... ADD PARTITION FIELD` afterwards does apply. Always
  confirm a partition spec with `SHOW TABLE` rather than trusting the absence of an
  error.

## 9. Databricks platform tests

`.github/workflows/databricks-tests.yml` runs `tests/databricks_feature_tests.py`
against a live Databricks SQL warehouse from the GitHub runner (like the
Redshift suite: API-driven, no cluster-side code). Tables are created as Unity
Catalog **managed Iceberg** tables in a catalog whose managed location sits on
the data bucket, so the suite can list the S3 prefix with boto3 and prove each
table is genuine Iceberg (`metadata/*.metadata.json`, no `_delta_log/`) rather
than Delta + UniForm.

The matrix cells for the `databricks` platform were measured on a DBSQL 2026.15
warehouse, serverless Spark 4.1.0 and a DBR 18.2 cluster. The report prints the
warehouse version of each run next to that reference, so a discrepancy on a
newer warehouse can be recognised as version drift instead of bad data.

### One-time setup

1. **AWS side** — redeploy the stack with your Databricks *account* id (the
   UUID in the account console URL), which creates the storage-credential role
   scoped to `s3://<data-bucket>/databricks/*`:

   ```bash
   aws cloudformation deploy --stack-name icebergmatrix-ci \
     --template-file infra/aws/aws.yaml \
     --capabilities CAPABILITY_NAMED_IAM \
     --parameter-overrides DatabricksAccountId=<databricks-account-uuid> \
       ... # existing parameters unchanged
   ```

   Note the `DatabricksStorageRoleArn` output.

2. **Databricks side** (workspace must be on a plan that allows external
   locations; Free Edition does not):

   ```sql
   -- Catalog Explorer → External data → Credentials → Create credential,
   -- with the DatabricksStorageRoleArn from the stack output. Then:
   CREATE EXTERNAL LOCATION icebergmatrix_ext
     URL 's3://<data-bucket>/databricks/'
     WITH (STORAGE CREDENTIAL <credential-name>);

   CREATE CATALOG icebergmatrix
     MANAGED LOCATION 's3://<data-bucket>/databricks/managed/';
   ```

   Also create (or pick) a serverless SQL warehouse and note its id from the
   Connection details tab.

   If `CREATE EXTERNAL LOCATION` fails with

   ```
   PERMISSION_DENIED: AWS IAM role does not have READ permissions on url
   s3://<bucket>/databricks. Cause: AccessDeniedException error from cloud
   storage provider.
   ```

   read it as a permissions problem, not a trust problem. The message names S3,
   so the role was assumed successfully and the external id already matches;
   only the identity policy is at fault. The first version of this stack scoped
   the bucket-level actions with `StringLike s3:prefix = databricks/*`, which
   fails twice over: `s3:GetBucketLocation` sends no `s3:prefix` key at all so a
   condition on it can never match, and Unity Catalog validates the location as
   written -- note the missing trailing slash in the error -- so a list with
   prefix `databricks` misses the pattern too. `iam simulate-principal-policy`
   settles which of the two it is in one call. The template now leaves the
   bucket statement unconditioned, as Databricks documents; object access stays
   scoped to `databricks/*`, so the credential can enumerate key names bucket
   wide but can only open objects under that prefix.

   Unity Catalog also needs the three multipart actions
   (`ListBucketMultipartUploads`, `ListMultipartUploadParts`,
   `AbortMultipartUpload`) for managed table writes. Encryption is the other
   thing worth ruling out early: on an SSE-KMS bucket the role needs
   `kms:Decrypt`/`Encrypt`/`GenerateDataKey*` *and* an entry in the key policy.
   Ours is SSE-S3, so there is no KMS section.

3. **GitHub side** — create a *separate* `databricks-live` environment, with its
   own required reviewer, and put every secret this workflow needs on it:

   | Secret | Value |
   |---|---|
   | `DATABRICKS_HOST` | `https://dbc-xxxx.cloud.databricks.com` |
   | `DATABRICKS_TOKEN` | PAT (or service-principal OAuth token) |
   | `DATABRICKS_WAREHOUSE_ID` | SQL warehouse id |
   | `AWS_CI_ROLE_ARN` | `CiRoleArn` output, for the S3 layout inspection |
   | `AWS_REGION` | the region the data bucket lives in |
   | `AWS_DATA_BUCKET` | your S3 bucket name |

   The last three are duplicated from `aws-live` rather than shared, because
   environment secrets are per-environment: a job can only read the secrets of the
   environment it gates on.

   These credentials used to live on `aws-live`, which was worse in two ways that
   are easy to miss. Environment secrets are readable by *any* job gating on that
   environment, so the AWS platform tests could read the Databricks token despite
   having no use for it; and a single approval queue covered two unrelated kinds of
   spend, so approving an EMR run and approving Databricks warehouse time were the
   same decision with the same audit trail.

   The split has one consequence worth knowing: the Databricks job still assumes
   the AWS CI role for the S3 inspection, and that role's trust is scoped by OIDC
   subject, so `databricks-live` has to be an allowed subject. That is what the
   `DatabricksEnvironmentName` parameter does. **Deploy the stack before the first
   dispatch from the new environment** — otherwise the run reaches
   `configure-aws-credentials` and fails there with "Not authorized to perform
   sts:AssumeRoleWithWebIdentity", which reads like a broken secret rather than a
   missing trust entry. Setting the parameter equal to `EnvironmentName` collapses
   the two environments back into one.

### Run it

Dispatch **Databricks Platform Tests** with `dry-run: true` first: it verifies
the SQL warehouse connection and the OIDC role without creating anything. A real
run creates per-run schemas `icebergmatrix-<run-id>_<n>` in the catalog, runs the
suite, and `tests/databricks_teardown.py` drops every `icebergmatrix*` schema
CASCADE afterwards (Unity Catalog purges managed storage asynchronously). The
`only` input runs a comma-filtered subset while iterating, e.g. `variant,lineage`.

### Costs

Serverless SQL warehouses bill per query with auto-stop; a full suite run is a
few minutes of warehouse time. The S3 layout inspection is read-only from the CI
role. Managed-table storage is purged on teardown; anything the teardown could
not remove is printed loudly in the job log.

## 10. Snowflake platform tests

`.github/workflows/snowflake-tests.yml` runs `tests/snowflake_feature_tests.py`
against a live Snowflake account from the GitHub runner (like the Databricks and
Redshift suites: API-driven through the Python connector, no warehouse-side
code). Tables are created as Snowflake-managed Iceberg tables
(`CATALOG = 'SNOWFLAKE'`) on an external volume whose storage sits under
`s3://<data-bucket>/snowflake/`, so the suite can list the S3 prefix with boto3
and prove each table is genuine Iceberg (`metadata/*.metadata.json`) rather than
trusting the catalog.

The matrix cells for the `snowflake` platform live in
`src/data/platforms/snowflake/snowflake/snowflake.json` and were measured on a
Snowflake Enterprise account in us-east-1 with Iceberg v3 GA. The report prints
the account's `CURRENT_VERSION()` next to that reference, so a discrepancy on a
newer account can be recognised as Snowflake having shipped a feature since
rather than bad data.

### The object that matters is the external volume, not a storage integration

Snowflake-managed Iceberg tables authenticate to S3 through an **external
volume**, and the external volume carries its own IAM trust relationship. A
storage integration is a *different* object (used for external stages and
`COPY`), and a managed Iceberg table does not use one at all. Do **not** create a
storage integration for this — it is not needed, and worse, it is an active trap:
a storage integration and an external volume each generate their *own*
`STORAGE_AWS_EXTERNAL_ID`, both naming the same Snowflake IAM user and pointing at
the same role. A single IAM role trust policy pins exactly one external id, so if
you wire the trust to the integration's id, the volume's assume is rejected with
`sts:AssumeRole ... is not authorized` — and the two ids look nearly identical,
so the mismatch is easy to stare straight past.

The volume's external id does not exist until the volume exists, and the volume
references the role by ARN, so the role has to exist first. That is a genuine
chicken-and-egg, resolved with a two-pass deploy. This is the one real difference
from the Databricks role, whose external id (the account UUID) you already know.

1. **AWS pass one** — deploy the stack opting in to the Snowflake role but with
   no external id yet. The role is created trusting only its own account with a
   sentinel external id that Snowflake never sends, so it exists but cannot be
   assumed:

   ```bash
   aws cloudformation deploy --stack-name icebergmatrix-ci \
     --template-file infra/aws/aws.yaml \
     --capabilities CAPABILITY_NAMED_IAM \
     --parameter-overrides SnowflakeEnvironmentName=snowflake-live \
       ... # existing parameters unchanged
   ```

   Note the `SnowflakeStorageRoleArn` output.

2. **Snowflake side** — create the external volume pointing at that role, then
   read back the two values AWS needs from the **volume** (not from any
   integration). `ALLOW_WRITES = TRUE` is required for managed tables:

   ```sql
   CREATE EXTERNAL VOLUME icebergmatrix_vol
     STORAGE_LOCATIONS = (
       (
         NAME = 'icebergmatrix-us-east-1'
         STORAGE_PROVIDER = 'S3'
         STORAGE_BASE_URL = 's3://<data-bucket>/snowflake/'
         STORAGE_AWS_ROLE_ARN = '<SnowflakeStorageRoleArn from step 1>'
       )
     )
     ALLOW_WRITES = TRUE;

   DESC EXTERNAL VOLUME icebergmatrix_vol;
   -- In the STORAGE_LOCATION_1 row, read STORAGE_AWS_IAM_USER_ARN and
   -- STORAGE_AWS_EXTERNAL_ID from the JSON. These are the volume's own values.
   ```

   `CREATE EXTERNAL VOLUME` does not itself assume the role, so it succeeds even
   though the trust is still the sentinel; the assume is not exercised until
   step 4.

3. **AWS pass two** — redeploy with both values, which rewrites the trust policy
   to admit exactly this volume (the Snowflake-owned IAM user as the principal,
   the volume's external id as the confused-deputy guard). Pass the external id
   with the `Key=Value` parameter form rather than `Key=Value` space-splitting,
   because the value contains `=` characters:

   ```bash
   aws cloudformation deploy --stack-name icebergmatrix-ci \
     --template-file infra/aws/aws.yaml \
     --capabilities CAPABILITY_NAMED_IAM \
     --parameter-overrides \
       SnowflakeEnvironmentName=snowflake-live \
       "SnowflakeIamUserArn=<STORAGE_AWS_IAM_USER_ARN>" \
       "SnowflakeExternalId=<STORAGE_AWS_EXTERNAL_ID>" \
       ... # existing parameters unchanged
   ```

   Both must be supplied together: the template only wires the real trust when
   `SnowflakeExternalId` is non-empty, and an external id without the matching
   IAM user ARN would trust the account root, which is wrong. If you ever recreate
   the volume (`CREATE OR REPLACE EXTERNAL VOLUME`), Snowflake issues a **new**
   external id, so repeat step 2's `DESC` and redeploy — a stale external id fails
   the assume at write time, not at create time, which is the confusing part.

4. **Verify the trust end to end** from Snowflake before relying on it. This is
   the checkpoint that catches a wrong external id, and it does a real
   write/read/list/delete round-trip against the bucket using the role:

   ```sql
   SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('icebergmatrix_vol');
   ```

   Wait for `"success":true` with `writeResult`/`readResult`/`listResult`/
   `deleteResult` all `PASSED`. A failure whose message is
   `Error assuming AWS_ROLE: ... is not authorized to perform: sts:AssumeRole`
   means the external id in the trust does not match the volume's — re-read
   `DESC EXTERNAL VOLUME` and compare the two ids character for character (the
   `STORAGE_AWS_EXTERNAL_ID` in the failure payload is the volume's current one).
   IAM trust edits take a few seconds to propagate, so if you have just
   redeployed, wait and re-run before assuming the value is wrong.

### The database and a managed table

With `VERIFY` green, create a database for the run schemas and confirm a real
managed table round-trips:

```sql
CREATE DATABASE IF NOT EXISTS ICEBERGMATRIX;

-- what the suite does per test, shown once by hand:
CREATE ICEBERG TABLE ICEBERGMATRIX.PUBLIC.probe (id INT, name STRING)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'icebergmatrix_vol'
  BASE_LOCATION = 'managed/probe';
INSERT INTO ICEBERGMATRIX.PUBLIC.probe VALUES (1, 'a');
SELECT * FROM ICEBERGMATRIX.PUBLIC.probe;
DROP ICEBERG TABLE ICEBERGMATRIX.PUBLIC.probe;
```

Match the external volume's region to the data bucket's region (us-east-1 here).
A cross-region volume works but pays egress on every read the suite does through
the layout inspection, and on Snowflake's own writes.

### Two things that are not obvious

**The lifecycle rule is a backstop, not the cleanup.** `s3-lifecycle.json`
expires `snowflake/managed/` after a day, but that is only for files orphaned by
a crashed run. For a managed table Snowflake owns the storage and tracks it
through its own catalog, so deleting those objects directly — with boto3 or by
letting the lifecycle rule reach a live table — corrupts Snowflake's view of the
table rather than cleaning it up. The real teardown is `DROP ICEBERG TABLE` /
`DROP SCHEMA ... CASCADE`, which `tests/snowflake_teardown.py` does and which
lets Snowflake remove its own files. The `snowflake/reports/` prefix keeps
reports for 30 days like the other engines.

**The warehouse and the external volume are long-lived.** Like the Redshift
Serverless workgroup and the Databricks SQL warehouse, the virtual warehouse
(auto-suspends, bills only while running) and the external volume are stack-level
infrastructure the suite only uses, never creates or drops. Teardown leaves both
alone.

### Configure the GitHub environment

Settings → Environments → `snowflake-live`. As with `databricks-live`, this is a
*separate* environment with its own required reviewer, so the Snowflake
credentials are not readable by the AWS or Databricks jobs, and approving
Snowflake warehouse time is its own decision with its own audit trail.

The Snowflake job still assumes the AWS CI role for the read-only S3 layout
inspection, and that role's trust is scoped by OIDC subject, so `snowflake-live`
has to be an allowed subject. That is what the `SnowflakeEnvironmentName`
parameter does (it is already set from pass one above). **Deploy the stack before
the first dispatch from the new environment** — otherwise the run reaches
`configure-aws-credentials` and fails there with "Not authorized to perform
sts:AssumeRoleWithWebIdentity", which reads like a broken secret rather than a
missing trust entry. Setting the parameter equal to `EnvironmentName` collapses
it back into `aws-live`.

- Required reviewer: yourself.
- Deployment branch policy: restrict to the branches you want to run from.
- Environment secrets (not repository secrets). Authenticate with either a
  password or a key pair — set `SNOWFLAKE_PASSWORD`, or set
  `SNOWFLAKE_PRIVATE_KEY` (PEM, with `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` if the
  key is encrypted). Key-pair auth is the better choice for CI because it does
  not expire the way a password policy can.

| Secret | Value |
|---|---|
| `SNOWFLAKE_ACCOUNT` | account identifier, e.g. `ab12345.us-east-1` |
| `SNOWFLAKE_USER` | login name the tests run as |
| `SNOWFLAKE_PASSWORD` | password auth (omit if using a key pair) |
| `SNOWFLAKE_PRIVATE_KEY` | PEM private key for key-pair auth (omit if using a password) |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | passphrase for the key, if it is encrypted |
| `SNOWFLAKE_ROLE` | role to use (optional; defaults to the user's default role) |
| `SNOWFLAKE_WAREHOUSE` | virtual warehouse for compute |
| `SNOWFLAKE_EXTERNAL_VOLUME` | `icebergmatrix_vol` (the volume from above) |
| `AWS_CI_ROLE_ARN` | `CiRoleArn` output, for the S3 layout inspection |
| `AWS_REGION` | the region the data bucket lives in |
| `AWS_DATA_BUCKET` | your S3 bucket name |

`AWS_CI_ROLE_ARN`, `AWS_REGION` and `AWS_DATA_BUCKET` are duplicated from
`aws-live` rather than shared, for the same reason they are duplicated onto
`databricks-live`: environment secrets are per-environment, so a job can only
read the secrets of the environment it gates on.

### Run it

Dispatch **Snowflake Platform Tests** with `dry-run: true` first: it verifies
the Snowflake connection and the OIDC role without creating anything. A real run
creates per-run schemas `ICEBERGMATRIX-<run-id>_<n>` in the database, runs the
suite, and `tests/snowflake_teardown.py` drops every `ICEBERGMATRIX*` schema
CASCADE afterwards (Snowflake removes the managed storage). The `only` input runs
a comma-filtered subset while iterating, e.g. `variant,lineage`.

### Costs

The virtual warehouse bills per second while a query runs and auto-suspends when
idle; a full suite run is a few minutes of warehouse time. The S3 layout
inspection is read-only from the CI role. Managed-table storage is removed on
teardown; anything teardown could not remove is printed loudly in the job log,
and the `snowflake/managed/` lifecycle rule expires it as a backstop.
