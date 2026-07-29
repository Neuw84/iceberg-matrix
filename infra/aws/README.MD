You should create a s3 bucket and s3 table bucket first. Then deploy the template. 

```
aws cloudformation deploy \
  --stack-name icebergmatrix-ci \
  --template-file tests/aws/bootstrap.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
      DataBucket=<your-bucket> \
      TableBucketArn=arn:aws:s3tables:<region>:<acct>:bucket/<name> \
      BudgetNotificationEmail=you@example.com```
```

Next, configure a lifecycle for the bucket. 

```
aws s3api put-bucket-lifecycle-configuration \
  --bucket iceberg-tests-matrix \
  --lifecycle-configuration file://infra/s3-lifecycle.json
```
