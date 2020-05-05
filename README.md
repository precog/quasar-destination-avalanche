# quasar-destination-avalanche [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

## Usage

```sbt
libraryDependencies += "com.precog" %% "quasar-destination-avalanche" % <version>
```

## Configuration

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
- `clientSecret` provided by Azure Active Directory
