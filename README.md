# quasar-destination-avalanche [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

## Usage

```sbt
libraryDependencies += "com.precog" %% "quasar-destination-avalanche-azure" % <version>
```

or

```sbt
libraryDependencies += "com.precog" %% "quasar-destination-avalanche-s3" % <version>
```

## Avalanche with Azure Blob Storage Configuration

The Avalanche destination uses Azure Blob Storage to stage files
before loading. Its only means of authentication is [Azure Active
Directory](https://azure.microsoft.com/en-us/services/active-directory/). It
has the following format:

```json
{
  "accountName": String,
  "containerName": String,
  "connectionUri": String,
  "clusterPassword": String,
  "writeMode": "create" | "replace" | "truncate",
  "credentials": Object
}
```


- `accountName` is the Azure storage account name to use.
- `containerName` is the name of the container that will be used to
  stage files before loading into Avalanche
- `connectionUri` is the JDBC URI provided by Actian
- `clusterPassword` is the password for `dbuser`
- `writeMode` determines the behaviour exhibited before table creation and loading, replace drops the table, truncate empties out the table's contents and create does nothing prior to table creation
- `credentials` specifies the Azure Active Directory configuration

```json
"credentials": {
  "clientId": String,
  "tenantId": String,
  "clientSecret": String
}
```

- `clientId` also called "Application Id"
- `tenantId` also called "Directory Id"
- `clientSecret` provided by Azure Active 

## Avalanche with AWS S3 Blob Storage Configuration

Configuration format for Avalanche with S3 staging is:

```json
{
  "bucketConfig": Object,
  "jdbcUri": String,
  "password": String,
  "writemode": "create" | "replace" | "truncate",
}
```

Where `bucketConfig has this format:

```json
{
  "bucket": String,
  "accessKey": String,
  "secretKey": String,
  "region": String
}
```

Example:

```json
{
  "bucketConfig": {
    "bucket": "bucket-name",
    "accessKey": "aws-access-key",
    "secretKey": "aws-secret-key",
    "region": "aws-bucket-region"},
  "jdbcUri": "jdbc:ingres://<avalanche-cluster-domain>:27839/db;encryption=on",
  "password": "avalanche-cluster-password",
  "writemode": "create"
}
```

### Deployment Requirements

In order to use S3 staging with Avalanche both must be in the same AWS region.

You must also create an IAM user, generate an AccessKey and SecretKey, and assign this policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VisualEditor0",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:DeleteObject",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::<bucket-name>/*",
        "arn:aws:s3:::<bucket-name>"
      ]
    },
    {
      "Sid": "VisualEditor1",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::<bucket-name>/*"
    }
  ]
}
```


