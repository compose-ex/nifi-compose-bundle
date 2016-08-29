# nifi-compose-bundle

An example NiFi Archive (.nar) with some custom Processors

### Quick Build and Deploy

  1. `git clone`
  2. `mvn package`
  3. Copy `nifi-compose-nar/target/nifi-compose-nar-1.0.0-BETA.nar` to `$NIFI_ROOT/lib` assuming you took the defaults from the nifi.properties file and that you are running [Apache Nifi](https://nifi.apache.org/) version 1.0.0-Beta.


For details on these custom Processors please see the following blog (especially the end): 
[What You Need to Know to Extend NiFi](https://www.compose.com/articles/what-you-need-to-know-to-extend-nifi)

For a general overview of Apache NiFi:
[Use NiFi to Lessen the Friction of Moving Data](https://www.compose.com/articles/lessen-the-friction-of-moving-data-with-nifi/)

Custom Processors in this bundle:

* `ComposeUniqueRocksDB` provides the ability to locally check for uniqueness of an attribute. It is a persisted index of keys. 
 Useful for local only unlike the de-duplication provided by NiFi via `DetectDuplicate` and its required Distributed Cache Service.
 
* `ComposeStreamingGetMongo` provides immediate creation of FlowFiles for each Document prior to the query being finished. This is
different than `GetMongo` which only calls `session.commit()` after the query is completed.

* `ComposeBatchPutMongo` exposes Mongo's `collection.insertMany()`. It takes a FlowFile that has a JSON
 array of Documents to put into Mongo.

* `ComposeTailingGetMongo` provides a snapshot of Mongo database plus a stream of oplog changes in the form of FlowFiles.

* `ComposeTailingPutMongo` puts the dump from the TailingGet into another Mongo.
