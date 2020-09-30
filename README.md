## Infinispan index/query load testing

This is a simple performance test for query/indexing that creates one or more Infinispan nodes in the same JVM. 
Current scenarios are query only, and a mix of query plus indexing.

The following configuration parameters can be used:
 
* ```indexmanager``` The index manager to use, either ```near-real-time``` or ```filesystem```. Default: ```near-real-time``` 
 * ```index_nodes``` The number of nodes in the cluster that will write data with `cache.put()`. Default: ```3```
 * ```query_nodes``` The number of nodes in the cluster that will execute queries, Default: ```1```
 * ```index_threads_per_node``` The number of threads for each writer node. Default: ```3```
 * ```query_threads_per_node``` The number of threads for each querying node. Default: ```1```
 * ```entries``` The number of entries to insert in the cache. Default: ```100_000```
 * ```phrase_size``` The number of words to add to the sample IndexedEntity description field. Default: ```10```
 * ```query_type``` The type of query to execute: ```TERM```, ```MATCH_ALL_PROJECTIONS```, ```MATCH_ALL```, ```SORT```. Default: ```TERM```
 * ```segments``` the number of Infinispan segments, defaults to ```256```
 * ```worker``` indexing backend worker, ```sync``` or ```async```, default: ```sync```
 * ```reader_strategy``` ```shared```, ```not-shared``` or ```async```. Default: ```shared```
 * ```reader_refresh``` if ```reader_strategy``` is async, will configure the reader refresh frequency in milliseconds. Default: ```100```
 * ```test``` the scenario to test. Either ```testQueryOnly``` or ```testQueryWithWrites```
 * ```backend``` worker: ```sync``` or ```async```

### Choosing Infinispan version

Change the ```<version.infinispan>``` property in the ```pom.xml```.

### Executing from maven

Use the execution id followed by one or more configuration parameters. 

Example, to run a query only test query with 1 nodes, 10 threads querying per node, 500k entries:

```mvn exec:java@queryOnly -Dentries=500000 -Dquery_nodes=1 -Dquery_threads_per_node=10``` 

### Test results

Each execution will print a summary such as:

```[Done in 2.262000s] 1 node(s), 5 thread(s) per node: Query 90th: 2264.924159, QPS: 2.210433]```

That includes the total duration of the test, the parameter(s) chosen, the 90th percentile of either query or index operations.
and a throughput measure.

### Profiling

#### Flight recorder

To enable flight recordings, export the MAVEN_OPTS variable:

```
export MAVEN_OPTS="-XX:StartFlightRecording=disk=true,dumponexit=true,filename=recording.jfr,maxsize=1024m,maxage=1d,settings=profile,path-to-gc-roots=true
```

#### YourKit

```
export MAVEN_OPTS="-agentpath:<YOURKIT_INSTALLATION>/bin/linux-x86-64/libyjpagent.so=tracing,onexit=snapshot,sessionname=<NAME>,dir=.,tracing"
```

