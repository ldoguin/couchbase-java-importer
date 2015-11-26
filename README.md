# couchbase-java-importer

This is a pluggable importer for Couchbase. So far it supports importing documents from a CSV file, a MongoDB or CouchDB instance.

## How to Build

```
$ ./gradlew build
```

## How to Use

```
$ cd build/distributions/
$ unzip couchbase-java-importer.zip
```

At this step you need to choose what kind of import you want to do. Configuration samples are available at the root of the repository. Let's pretend you want to import a MongoDB collection called `restaurants` from the database `test` and add a field called `couchbaseType` with the value `restaurant` for every imported documents. Let's also pretend you have a local MongoDB and local Couchbase instance. For that you would need the following configuration

```
# Hostnames, comma separated list of Couchbase node IP or hostname
hostnames: localhost,127.0.0.1
# Buket name
bucket: default
# Bucket password
password:
# Log to write succesfully imported keys
successLogFilename: succes.out
# Log to write unsuccesfully imported keys
errorLogFilename: error.out
# Default RequestCancelledException delay in milliseconds and maximum number of retries
requestCancelledExceptionDelay: 31000
requestCancelledExceptionRetries: 100
# Default TemporaryFailureException delay in milliseconds and maximum number of retries
temporaryFailureExceptionDelay: 100
temporaryFailureExceptionRetries: 100
# Default upsert timeout in milliseconds
importTimeout: 500
# Choose between CSV, COUCHDB, MONGODB
choosenImporter: MONGODB
mongodb:
  # Give a valid connection string to connect to a MongoDB instance
  connectionString: "mongodb://127.0.0.1:27017/"
  # Name of the MondoDB database to connect to
  dbName: "test"
  # Name of the collection to import
  collectionName: "restaurants"
  # Couchbase does not have collection, we usually use a type field. As there could already be a type field in Mongo, you can specify another fieldName to be used as type
  typeField: "type"
  # type of the documents that will be imported
  type: "restaurant"
```

This is the content of the MongoDB sample configuration. To run the import copy the configuration file and run the importer:

```
$ cp ../../../application-mongodb.yml.sample application.yml 
$ ./bin/couchbase-java-importer
```

Once the import as ran you should have one file called `success.out` that contains the id of every document imported. If something went wrong you should also have a file called `error.out`.

Every configuration samples contains comments that should help you understand the various import options.
