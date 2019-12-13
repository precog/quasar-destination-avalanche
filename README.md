# quasar-destination-avalanche [![Build Status](https://travis-ci.com/slamdata/quasar-destination-avalanche.svg?branch=master)](https://travis-ci.com/slamdata/quasar-destination-avalanche) [![Bintray](https://img.shields.io/bintray/v/slamdata-inc/maven-public/quasar-destination-avalanche.svg)](https://bintray.com/slamdata-inc/maven-public/quasar-destination-avalanche) [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

## Usage

```sbt
libraryDependencies += "com.slamdata" %% "quasar-destination-avalanche" % <version>
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
  "credentials": Object
}
```


- `accountName` is the Azure storage account name to use.
- `containerName` is the name of the container that will be used to
  stage files before loading into Avalanche
- `connectionUri` is the JDBC URI provided by Actian
- `clusterPassword` is the password for `dbuser`
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
