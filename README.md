# nifi-compose-bundle

An example NiFi Archive (.nar) with some custom Processors

### Quick Build and Deploy

  1. `git clone`
  2. `mvn package`
  3. Copy `nifi-compose-nar/target/nifi-compose-nar-1.0.0-BETA.nar` to `$NIFI_ROOT/lib` assuming you took the defaults from the nifi.properties file and that you are running [Apache Nifi](https://nifi.apache.org/) version 1.0.0-Beta.


For details on these custom Processors please see the following blog (especially the end): 
[What You Need to Know to Extend NiFi](https://www.compose.com/articles/what-you-need-to-know-to-extend-nifi)
