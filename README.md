# quasar-destination-avalanche [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

## Usage

```sbt
libraryDependencies += "com.precog" %% "quasar-destination-avalanche-azure" % <version>
```

or

```sbt
libraryDependencies += "com.precog" %% "quasar-destination-avalanche-s3" % <version>
```

or

```sbt
libraryDependencies += "com.precog" %% "quasar-destination-avalanche-http" % <version>
```

## Avalanche with Azure Blob Storage Configuration

The Avalanche destination uses Azure Blob Storage to stage files before loading. Its only means of authentication is [Azure Active Directory](https://azure.microsoft.com/en-us/services/active-directory/). It has the following format:

```
{
  "accountName": String,
  "containerName": String,
  "connectionUri": String,
  "username":String,
  "clusterPassword": String,
  "externalAuth": Object,
  "writeMode": "create" | "replace" | "truncate",
  "credentials": Object
}
```

- `accountName` is the Azure storage account name to use.
- `containerName` is the name of the container that will be used to
  stage files before loading into Avalanche
- `connectionUri` is the JDBC URI provided by Actian
- `username` username when using username/password authentication. Must be omitted if using `externalAuth`
- `clusterPassword` is the password associated with the username. Must be omitted if using `externalAuth`
- `externalAuth` is the external authentication configuration when using SSO. Example given below. Must be omitted if using `username` and `clusterPassword`
- `writeMode` determines the behaviour exhibited before table creation and loading, replace drops the table, truncate empties out the table's contents and create does nothing prior to table creation
- `credentials` specifies the Azure Active Directory configuration

```
"credentials": {
  "clientId": String,
  "tenantId": String,
  "clientSecret": String
}
```

- `clientId` also called "Application Id"
- `tenantId` also called "Directory Id"
- `clientSecret` provided by Azure Active 

External authentication:

```
"externalAuth": {
  "authId": String,
  "userinfoUri": String, 
  "userinfoUidField": String
}
```

- `authId`: identifier obtained from the quasar implementation representing the externally obtained bearer token
- `userinfoUri`: the uri which can be queried using the access token obtain with the `authId` identifier to obtain the UID to be used in the JDBC string
- `userinfoUidField`: the field name that corresponds to a field on the `userinfoUri` response. The value of the field under this name will be inserted into the JDBC string as the UID.

## Avalanche with AWS S3 Blob Storage Configuration

Configuration format for Avalanche with S3 staging is:

```
{
  "bucketConfig": Object,
  "connectionUri": String,
  "username":String,
  "clusterPassword": String,
  "externalAuth": Object,
  "writeMode": "create" | "replace" | "truncate",
}
```

- `connectionUri` is the JDBC URI provided by Actian
- `username` username when using username/password authentication. Must be omitted if using `externalAuth`
- `clusterPassword` is the password associated with the username. Must be omitted if using `externalAuth`
- `externalAuth` is the external authentication configuration when using SSO. Example given below. Must be omitted if using `username` and `clusterPassword`
- `writeMode` determines the behaviour exhibited before table creation and loading, replace drops the table, truncate empties out the table's contents and create does nothing prior to table creation

External authentication:

```
"externalAuth": {
  "authId": String,
  "userinfoUri": String, 
  "userinfoUidField": String
}
```

- `authId`: identifier obtained from the quasar implementation representing the externally obtained bearer token
- `userinfoUri`: the uri which can be queried using the access token obtain with the `authId` identifier to obtain the UID to be used in the JDBC string
- `userinfoUidField`: the field name that corresponds to a field on the `userinfoUri` response. The value of the field under this name will be inserted into the JDBC string as the UID.

`bucketConfig` has this format:

```
{
  "bucket": String,
  "credentials": {
    "accessKey": String,
    "secretKey": String,
    "region": String
  }
}
```

Example:

```
{
  "bucketConfig": {
    "bucket": "bucket-name",
    "credentials": {
      "accessKey": "aws-access-key",
      "secretKey": "aws-secret-key",
      "region": "aws-bucket-region"
    }
  },
  "connectionUri": "jdbc:ingres://<avalanche-cluster-domain>:27839/db;encryption=on",
  "username": "dbuser",
  "clusterPassword": "avalanche-cluster-password",
  "writeMode": "create"
}
```

### Deployment Requirements

In order to use S3 staging with Avalanche both must be in the same AWS region.

You must also create an IAM user, generate an AccessKey and SecretKey, and assign this policy:

```
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

## Avalanche with Direct HTTP Configuration

When the Avalanche cluster has direct access via HTTP to Precog, staging can be avoided altogether. Configuration for this variant is:

```
{
  "connectionUri": String,
  "username":String,
  "clusterPassword": String,
  "writeMode": "create" | "replace" | "truncate",
  ["baseUrl": String]
}
```

- `connectionUri` is the JDBC URI provided by Actian
- `username` username when using username/password authentication. Must be omitted if using `externalAuth`
- `clusterPassword` is the password associated with the username. Must be omitted if using `externalAuth`
- `externalAuth` is the external authentication configuration when using SSO. Example given below. Must be omitted if using `username` and `clusterPassword`
- `writeMode` determines the behaviour exhibited before table creation and loading, replace drops the table, truncate empties out the table's contents and create does nothing prior to table creation
- `baseUrl` (optional): The base URL that should be used to access the Precog HTTP api, if omitted the IP and port of the Precog server itself is used.

External authentication:

```
"externalAuth": {
  "authId": String,
  "userinfoUri": String, 
  "userinfoUidField": String
}
```

- `authId`: identifier obtained from the quasar implementation representing the externally obtained bearer token
- `userinfoUri`: the uri which can be queried using the access token obtain with the `authId` identifier to obtain the UID to be used in the JDBC string
- `userinfoUidField`: the field name that corresponds to a field on the `userinfoUri` response. The value of the field under this name will be inserted into the JDBC string as the UID.

Example:

```
{
  "connectionUri": "jdbc:ingres://<avalanche-cluster-domain>:27839/db;encryption=on",
  "username": "dbuser",
  "clusterPassword": "avalanche-cluster-password",
  "writeMode": "create",
  "baseUrl": "https://precog.myorganization.com"
}
```
