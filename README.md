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

At this step you need to choose what kind of import you want to do. Configuration samples are available at the root of the repository.

Let's pretend you want to import a MongoDB collection called `restaurants` from the database `test` and add a field called `couchbaseType` with the value `restaurant` for every imported documents. Let's also pretend you have a local MongoDB and local Couchbase instance. For that you would need the following configuration

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

## JDBC Configuration

Start by copying the JDBC sample configuration:

```
 cp ../../../application-jdbc.yml.sample application.yml 
```

Make sure you have configured your JDBC connection through the following properties:

```
spring.jpa.database=POSTGRESQL
spring.datasource.platform=postgres
spring.jpa.show-sql=true
spring.jpa.hibernate.ddl-auto=create-drop
spring.database.driverClassName=org.postgresql.Driver
spring.datasource.url=jdbc:postgresql://192.168.99.100:32778/dvdrental
spring.datasource.username=postgres
spring.datasource.password=password
```

More information available on the [spring documentation](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/jdbc.html).

Once you have your connection setup, you can configure what you want to import using the following filters used by the [DatabaseMetaData Object](https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html).

```
choosenImporter: JDBC
jdbc:
  catalog: 
  schemaPattern: "public"
  tableNamePattern: 
  types:
  columnNamePattern:
```

All the selected tables and columns will be stored in a JsonDocument. It's key is defined by the tablesSchemaId property.

```
jdbc:
  # id of the document containing your database schema to be imported 
  tablesSchemaId: myDatabaseSchema
```

## CouchDB

Start by copying the JDBC sample configuration:

```
 cp ../../../application-jdbc.yml.sample application.yml 
```

Then use the downloadURL property to define wich CouchDB documents you want to import. By default it uses the all_docs view with the include_docs flag set to true. 

```
choosenImporter: COUCHDB
couchdb:
  # Download URL 
  downloadURL: http://127.0.0.1:5984/database_export/_all_docs?include_docs=true
```

## CSV

Sample configuration for CSV import:

```
choosenImporter: CSV
csv:
  # CSV Separating char for rows
  columnSeparator: ';'
  # CSV quotes
  quoteChar: ''
  # Path to the CSV file to import
  csvFilePath: /home/couchbase/csvimporter/advocates.csv
  # Skip the first line of the CSV for field names
  skipFirstLineForNames: true
  # Any format usable by the Java SimpleDateFormat Class
  dateFormat: EEE MMM dd HH:mm:ss z yyyy
  # Language tag used by Java's Locale class
  languageTag: FR_FR
  # Number of columns to import
  totalcolumns: 10
  # Column index to use the column value as id
  keyColumIndex: 0
  # The value of this field will be added as key prefix
  keyPrefix: "advocate::"
  #Give the type of the columns, could be String, Long, Double, Boolean, Date. Must be the exact same size as the number of columns in your file
  columType:
     - STRING
     - STRING
     - STRING
     - STRING
     - STRING
     - DATE
     - LONG
  # Choose the name of the fields for each column, mandatory if skipFirstLineForNames is set to true.
  columName:
     - id
     - type
     - firstname
     - lastname
     - location
     - creationDate
     - count
```

### MongoDB
