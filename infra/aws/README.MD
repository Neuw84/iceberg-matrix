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
