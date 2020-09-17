package org.infinispan.query;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.IntStream.range;
import static java.util.stream.IntStream.rangeClosed;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.infinispan.Cache;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.test.TestingUtil;

public class QueryPerf {

   private static final String DEFAULT_INDEX_MANAGER = "near-real-time";
   private static final int DEFAULT_NUM_ENTRIES = 100_000;
   private static final int DEFAULT_NUM_SEGMENTS = 256;
   private static final int DEFAULT_NUM_OWNERS = 2;
   private static final int DEFAULT_REMOTE_TIMEOUT_MINUTES = 1;
   private static final int DEFAULT_INDEXING_THREADS_PER_NODE = 3;
   private static final int DEFAULT_QUERYING_THREADS_PER_NODE = 1;
   private static final int DEFAULT_INDEXING_NODES = 3;
   private static final int DEFAULT_QUERYING_NODES = 1;
   private static final String DEFAULT_READER_REFRESH_MS = "100";
   private static final String DEFAULT_WORKER = "sync";
   private static final String DEFAULT_READER = "shared";
   private static final QueryType DEFAULT_QUERY_TYPE = QueryType.TERM;

   private static final String ENTRIES_SYS_PROP = "entries";
   private static final String INDEX_MANAGER_SYS_PROP = "indexmanager";
   private static final String SEGMENTS_SYS_PROP = "segments";
   private static final String INDEX_THREADS_SYS_PROP = "index_threads_per_node";
   private static final String QUERY_THREADS_SYS_PROP = "query_threads_per_node";
   private static final String WORKER_SYS_PROP = "worker";
   private static final String QUERY_TYPE_SYS_PROP = "query_type";
   private static final String INDEXING_NODES_SYS_PROP = "index_nodes";
   private static final String QUERYING_NODES_SYS_PROP = "query_nodes";
   private static final String READER_SYS_PROP = "reader_strategy";
   private static final String READER_REFRESH = "reader_refresh";

   public static final String COMMIT_INTERVAL = "index_defaults.io.commit_interval";
   public static final String IO_MERGE_FACTOR = "index_defaults.io.merge.factor";
   public static final String IO_WRITER_RAM_BUFFER_SIZE = "index_defaults.io.writer.ram_buffer_size";
   public static final String IO_MERGE_MAX_SIZE = "index_defaults.io.merge.max_size";
   public static final String IO_STRATEGY = "index_defaults.io.strategy";
   public static final String NEAR_REAL_TIME = "near-real-time";
   public static final String SHARDING_STRATEGY = "index_defaults.sharding.strategy";
   public static final String HASH = "hash";
   public static final String NUMBER_OF_SHARDS = "index_defaults.sharding.number_of_shards";
   public static final String INDEX_DIR = "target/";

   protected Random random = new Random();

   List<Cache<String, IndexedEntity>> caches = new ArrayList<>();
   List<EmbeddedCacheManager> cacheManagers = new ArrayList<>();

   protected String getIndexManager() {
      return System.getProperty(INDEX_MANAGER_SYS_PROP, DEFAULT_INDEX_MANAGER);
   }

   protected int getNumEntries() {
      return Integer.getInteger(ENTRIES_SYS_PROP, DEFAULT_NUM_ENTRIES);
   }

   protected int getNumSegments() {
      return Integer.getInteger(SEGMENTS_SYS_PROP, DEFAULT_NUM_SEGMENTS);
   }

   protected int getRemoteTimeoutInMinutes() {
      return DEFAULT_REMOTE_TIMEOUT_MINUTES;
   }

   protected int getIndexThreadsPerNode() {
      return Integer.getInteger(INDEX_THREADS_SYS_PROP, DEFAULT_INDEXING_THREADS_PER_NODE);
   }

   protected int getQueryThreadsPerNode() {
      return Integer.getInteger(QUERY_THREADS_SYS_PROP, DEFAULT_QUERYING_THREADS_PER_NODE);
   }

   protected int getQueryingNodes() {
      return Integer.getInteger(QUERYING_NODES_SYS_PROP, DEFAULT_QUERYING_NODES);
   }

   protected int getIndexingNodes() {
      return Integer.getInteger(INDEXING_NODES_SYS_PROP, DEFAULT_INDEXING_NODES);
   }

   protected String getWorker() {
      return System.getProperty(WORKER_SYS_PROP, DEFAULT_WORKER);
   }

   protected String getReaderStrategy() {
      return System.getProperty(READER_SYS_PROP, DEFAULT_READER);
   }

   protected String getReaderRefresh() {
      return System.getProperty(READER_REFRESH, DEFAULT_READER_REFRESH_MS);
   }

   protected QueryType getQueryType() {
      String sysProp = System.getProperty(QUERY_TYPE_SYS_PROP);
      return sysProp == null ? DEFAULT_QUERY_TYPE : QueryType.valueOf(sysProp.toUpperCase());
   }

   protected ConfigurationBuilder getDefaultCacheConfigBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).transaction().cacheStopTimeout(0L);
      builder.statistics().enable();
      builder.indexing().enabled(true)
            .addIndexedEntities(IndexedEntity.class)
            // ISPN 11 properties
            .addProperty("default.indexmanager", "near-real-time")
            .addProperty("default.indexBase", INDEX_DIR)
            .addProperty("default.indexwriter.merge_factor", "30")
            .addProperty("default.indexwriter.merge_max_size", "1024")
            .addProperty("default.indexwriter.ram_buffer_size", "256")
            // ISPN 12 properties
            .addProperty("directory.root", INDEX_DIR)
            .addProperty(IO_MERGE_FACTOR, "30")
            .addProperty(IO_MERGE_MAX_SIZE, "1024")
            .addProperty(IO_WRITER_RAM_BUFFER_SIZE, "256")
      ;

//      props.put("default.worker.execution", getWorker());
//      props.put("default.reader.strategy", getReaderStrategy());
//      props.put("default.reader.async_refresh_period_ms", getReaderRefresh());

      return builder;
   }

   protected int getNumOwners() {
      return DEFAULT_NUM_OWNERS;
   }

   synchronized Cache<String, IndexedEntity> pickCache() {
      return caches.get(random.nextInt(caches.size()));
   }

   protected void assertDocsIndexed() {
      int numEntries = getNumEntries();
      this.eventually(() -> {
         Query<Object[]> query = Search.getQueryFactory(pickCache()).create("FROM org.infinispan.query.IndexedEntity");
         QueryResult<Object[]> execute = query.maxResults(10).execute();
         return execute.hitCount().orElse(-1) == numEntries;
      });
   }

   void populate(int initialId, int finalId) {
      rangeClosed(initialId, finalId).forEach(i -> pickCache().put(String.valueOf(i), new IndexedEntity("name" + i, "desc " + i)));
   }

   synchronized void addNode() {
      addClusterEnabledCacheManager(getDefaultCacheConfigBuilder());
      waitForClusterToForm();
   }

   protected void waitForClusterToForm() {
      Cache<String, IndexedEntity> cache = caches.get(0);
      TestingUtil.blockUntilViewsReceived(30000, caches);
      if (cache.getCacheConfiguration().clustering().cacheMode().isClustered()) {
         TestingUtil.waitForNoRebalance(caches);
      }
   }

   protected void eventually(Supplier<Boolean> condition) {
      try {
         long timeoutNanos = TimeUnit.SECONDS.toNanos(10);
         // We want the sleep time to increase in arithmetic progression
         // 30 loops with the default timeout of 30 seconds means the initial wait is ~ 65 millis
         int loops = 30;
         int progressionSum = loops * (loops + 1) / 2;
         long initialSleepNanos = timeoutNanos / progressionSum;
         long sleepNanos = initialSleepNanos;
         long expectedEndTime = System.nanoTime() + timeoutNanos;
         while (expectedEndTime - System.nanoTime() > 0) {
            if (condition.get())
               return;
            LockSupport.parkNanos(sleepNanos);
            sleepNanos += initialSleepNanos;
         }
         if (!condition.get()) {
            throw new RuntimeException("Timeout waiting for condition");
         }
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!", e);
      }
   }

   /**
    * Represents a member of the cluster that executes some load test.
    */
   abstract class Node {
      protected EmbeddedCacheManager cacheManager;
      protected Cache<String, IndexedEntity> cache;
      final int WARMUP_ITERATIONS = 1000;
      final LatencyStats latencyStats = new LatencyStats();

      Node addToCluster() {
         cacheManager = addClusterEnabledCacheManager(getDefaultCacheConfigBuilder());
         cache = cacheManager.getCache();
         caches.add(cache);
         return this;
      }

      void kill() {
         cache.clear();
         cacheManager.stop();
         cacheManagers.remove(cacheManager);
         latencyStats.getPauseDetector().shutdown();
         latencyStats.stop();
      }

      abstract CompletableFuture<Void> run();

      abstract void warmup();

      NodeSummary getNodeSummary(long timeMs) {
         Histogram intervalHistogram = latencyStats.getIntervalHistogram();
         return new NodeSummary(intervalHistogram, timeMs);
      }

   }

   /**
    * A {@link Node} that executes some task using multiple threads bounded by a fixed thread pool
    */
   abstract class TaskNode extends Node {
      private final ExecutorService executorService;
      private final int nThreads;
      protected AtomicInteger globalCounter;

      TaskNode(int nThreads, AtomicInteger globalCounter) {
         executorService = Executors.newFixedThreadPool(nThreads, r -> {
            Thread thread = new Thread(r);
            thread.setName("TaskNode");
            return thread;
         });
         this.nThreads = nThreads;
         this.globalCounter = globalCounter;
      }

      void kill() {
         executorService.shutdownNow();
         super.kill();
      }

      abstract void executeTask();

      abstract void warmup();

      CompletableFuture<Void> run() {
         List<CompletableFuture<?>> futures = range(0, nThreads).boxed().map(t -> supplyAsync(() -> {
            executeTask();
            return null;
         }, executorService)).collect(Collectors.toList());
         return CompletableFuture.allOf(futures.toArray(new CompletableFuture[nThreads]));
      }
   }

   /**
    * A {@link Node} that simply generated data and indexes data.
    */
   class IndexingNode extends TaskNode {
      IndexingNode(int nThreads, AtomicInteger globalCounter) {
         super(nThreads, globalCounter);
      }

      @Override
      void executeTask() {
         int id = 0;
         int numEntries = getNumEntries();
         while (id <= numEntries) {
            id = globalCounter.incrementAndGet();
            if (id <= numEntries) {
               long start = System.nanoTime();
               cache.put(String.valueOf(id), new IndexedEntity("name" + id, "desc " + id));
               if (id % 1000 == 0)
                  System.out.println("Put " + id);
               latencyStats.recordLatency(System.nanoTime() - start);
            }
         }
      }

      @Override
      void warmup() {
         for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            cache.put(String.valueOf(random.nextInt(WARMUP_ITERATIONS)), new IndexedEntity("name" + i, "desc " + i));
            if (i % 100 == 0) {
               System.out.printf("[Warmup] Added %d entries\n", i);
            }
         }
      }
   }

   enum QueryType {MATCH_ALL, TERM}

   /**
    * A {@link Node} that executes a fixed amount of queries against the cache.
    */
   class QueryingNode extends TaskNode {

      static final int QUERY_INTERVAL_MS = 10;
      final QueryType queryType;
      volatile Query<IndexedEntity> query;

      QueryingNode(int nThreads, AtomicInteger globalCounter, QueryType queryType) {
         super(nThreads, globalCounter);
         this.queryType = queryType;
      }

      Query<IndexedEntity> getQuery() {
         return Search.getQueryFactory(cache).create(createLuceneQuery());
//         if (query == null) {
//            synchronized (this) {
//               if (query == null) {
//                  query = Search.getQueryFactory(cache).create(createLuceneQuery());
//                  query.maxResults(10);
//               }
//            }
//         }
//         return query;
      }


      protected String createLuceneQuery() {
         if (queryType == QueryType.MATCH_ALL) {
            return "FROM org.infinispan.query.IndexedEntity";
         }
         if (queryType == QueryType.TERM) {
            return "SELECT name FROM org.infinispan.query.IndexedEntity";
         }
         return null;
      }

      protected int getRandomTerm() {
         return Math.round((float) (globalCounter.get() * 0.75));
      }

      @Override
      void executeTask() {
         int id = globalCounter.get();
         int numEntries = getNumEntries();
         while (id <= numEntries) {
            Query<IndexedEntity> query = getQuery();
            long start = System.nanoTime();
            List<IndexedEntity> list = query.execute().list();
            latencyStats.recordLatency(System.nanoTime() - start);
            id = globalCounter.get();
            LockSupport.parkNanos(QUERY_INTERVAL_MS * 1000_000L);
         }
      }

      @Override
      void warmup() {
         Query<?> query = Search.getQueryFactory(cache).create("SELECT name FROM org.infinispan.query.IndexedEntity").maxResults(100);
         for (int i = 1; i <= WARMUP_ITERATIONS; i++) {
            System.out.print(i + "\r");
            query.execute().list();
         }
      }

   }


   /**
    * Same as {@link QueryingNode} but time bound.
    */
   class TimeBoundQueryNode extends QueryingNode {

      private final long durationNanos;
      private final long waitIntervalNanos;

      TimeBoundQueryNode(long totalTime, TimeUnit totalTimeUnit, long waitBetweenQueries, TimeUnit waiTimeUnit,
                         int nThreads, QueryType queryType) {
         super(nThreads, null, queryType);
         this.waitIntervalNanos = TimeUnit.NANOSECONDS.convert(waitBetweenQueries, waiTimeUnit);
         this.durationNanos = TimeUnit.NANOSECONDS.convert(totalTime, totalTimeUnit);
      }

      @Override
      protected int getRandomTerm() {
         return random.nextInt(getNumEntries());
      }

      @Override
      void executeTask() {
         long now = System.nanoTime();
         long timeLimit = now + durationNanos;
         while (timeLimit - System.nanoTime() > 0L) {
            long start = System.nanoTime();
            QueryResult<IndexedEntity> queryResult = getQuery().execute();
            int size = queryResult.list().size();
            latencyStats.recordLatency(System.nanoTime() - start);
            LockSupport.parkNanos(waitIntervalNanos);
         }
      }
   }

   /**
    * Same as {@link QueryingNode} but executes a fixed number of queries.
    */
   class FixedNumberQueryNode extends QueryingNode {

      private final long totalQueries;

      FixedNumberQueryNode(long totalQueries, int nThreads, QueryType queryType) {
         super(nThreads, null, queryType);
         this.totalQueries = totalQueries;
      }

      @Override
      protected int getRandomTerm() {
         return random.nextInt(getNumEntries());
      }

      @Override
      void executeTask() {
         for (int i = 0; i < totalQueries; i++) {
            long start = System.nanoTime();
            QueryResult<IndexedEntity> queryResult = getQuery().execute();
            System.out.println("List size " + queryResult.list().size());
            System.out.println("Hit count " + queryResult.hitCount());

            latencyStats.recordLatency(System.nanoTime() - start);
         }
      }

      @Override
      void warmup() {
      }
   }

   /**
    * Summarizes performance data from a {@link Histogram} for displaying.
    */
   class NodeSummary {
      private final Histogram histogram;
      private final long totalTimeMs;

      NodeSummary(Histogram histogram, long totalTimeMs) {
         this.histogram = histogram;
         this.totalTimeMs = totalTimeMs;
      }

      double getOpsPerSecond() {
         return histogram.getTotalCount() / (totalTimeMs / 1.0E3);
      }

      double getValueAtPercentile(int percentile) {
         return histogram.getValueAtPercentile(percentile) / 1.0E6;
      }

      void outputHistogram() {
         histogram.outputPercentileDistribution(System.out, 1000000.0);
      }

   }

   private final AtomicInteger globalCounter = new AtomicInteger(0);
   private final List<Node> nodes = new ArrayList<>();

   public static void main(String[] args) throws Exception {
      QueryPerf indexManagerPerfTest = new QueryPerf();
      String usage = "Usage: IndexManagerPerf [opts] queryOnly | queryWithWrites";
      if (args.length != 1) {
         System.out.println(usage);
         System.exit(1);
      }
      if (args[0].equalsIgnoreCase("queryOnly")) {
         indexManagerPerfTest.testQueryOnly();
      } else if (args[0].equalsIgnoreCase("queryWithWrites")) {
         indexManagerPerfTest.testQueryWithWrites();
      } else {
         System.out.println(usage);
         System.exit(1);
      }
   }

   public void testQueryWithWrites() {
      nodes.addAll(range(0, getIndexingNodes()).boxed()
            .map(i -> new IndexingNode(getIndexThreadsPerNode(), globalCounter)).collect(Collectors.toList()));
      nodes.addAll(range(0, getQueryingNodes()).boxed()
            .map(i -> new QueryingNode(getQueryThreadsPerNode(), globalCounter, getQueryType())).collect(Collectors.toList()));
      nodes.forEach(Node::addToCluster);
      waitForClusterToForm();
      warmup();
      nodes.get(0).cacheManager.getCache().clear();

      summarizeReadWriteTest(runTests());
   }

   public void testQueryOnly() throws Exception {
      nodes.addAll(range(0, getQueryingNodes()).boxed()
            .map(i -> new TimeBoundQueryNode(60L, TimeUnit.SECONDS, 10L, TimeUnit.MILLISECONDS,
                  getQueryThreadsPerNode(), getQueryType()))
            .collect(Collectors.toList()));
      nodes.forEach(Node::addToCluster);


      waitForClusterToForm();
      addDataToCluster(getNumEntries(), ProcessorInfo.availableProcessors() * 2);
      warmup();

      summarizeQueryOnlyTest(runTests());
      after();
   }

   private void summarizeReadWriteTest(long totalTimeMs) {
      Stream<NodeSummary> queryNodesSummary = getNodeSummary(QueryingNode.class, totalTimeMs);
      Stream<NodeSummary> nodeSummary = getNodeSummary(IndexingNode.class, totalTimeMs);

      nodeSummary.forEach(NodeSummary::outputHistogram);

      Optional<double[]> queryStats = averageQueryStats(queryNodesSummary);
      String query90th = queryStats.map(u -> String.valueOf(u[0])).orElse("N/A");
      String qps = queryStats.map(u -> String.valueOf(u[1])).orElse("N/A");

      double totalTimeSeconds = totalTimeMs / 1000.0d;

      Double writeOpsPerSecond = getNumEntries() / (totalTimeMs / 1000.0d);

      System.out.printf("[Done in %fs] Index thread per node: %d, Query 90th: %s, QPS: %s, Put/s: %f\n",
            totalTimeSeconds, getIndexThreadsPerNode(), query90th, qps, writeOpsPerSecond);
   }

   private void summarizeQueryOnlyTest(long totalTimeMs) {
      Stream<NodeSummary> queryNodesSummary = getNodeSummary(QueryingNode.class, totalTimeMs);

      double[] queryStats = averageQueryStats(queryNodesSummary).orElse(new double[]{});
      double totalTimeSeconds = totalTimeMs / 1000.0d;

      System.out.printf("[Done in %fs] %d node(s), %d thread(s) per node: " +
                  "Query 90th: %f, QPS: %f\n",
            totalTimeSeconds, getQueryingNodes(), getQueryThreadsPerNode(), queryStats[0], queryStats[1]);
   }

   private Stream<NodeSummary> getNodeSummary(Class<? extends Node> type, long totalTimeMs) {
      return nodes.stream().filter(n -> type.isAssignableFrom(n.getClass())).map(node -> node.getNodeSummary(totalTimeMs));
   }

   private Optional<double[]> averageQueryStats(Stream<NodeSummary> queryNodesSummary) {
      return queryNodesSummary.map(n -> new double[]{n.getValueAtPercentile(90), n.getOpsPerSecond()})
            .reduce((a, b) -> new double[]{(a[0] + b[0]) / 2, b[0] + b[1]});
   }


   public void after() {
      nodes.forEach(Node::kill);
   }

   private long runTests() {
      long start = System.currentTimeMillis();
      nodes.stream().map(Node::run).parallel().forEach(CompletableFuture::join);
      long stop = System.currentTimeMillis();
      assertDocsIndexed();
      return stop - start;
   }

   private void addDataToCluster(int entries, int threads) throws Exception {
      System.out.println("Adding data to the cluster");
      ExecutorService executorService = Executors.newFixedThreadPool(threads);
      for (Node node : nodes) {
         Cache<String, IndexedEntity> cache = node.cache;
         CompletableFuture<?>[] futures = new CompletableFuture[threads];
         for (int i = 0; i < threads; i++) {
            futures[i] = CompletableFuture.supplyAsync(() -> {
               int id;
               do {
                  id = globalCounter.incrementAndGet();
                  if (id <= entries) {
                     cache.put(String.valueOf(id), new IndexedEntity("name" + id, "desc" + id));
                     if (id % 1000 == 0)
                        System.out.print(id + "\r");
                  }
               }
               while (id <= entries);
               return null;
            }, executorService);
         }
         CompletableFuture.allOf(futures).get(3, TimeUnit.MINUTES);
      }
      executorService.shutdownNow();
      System.out.println();
   }

   private void warmup() {
      System.out.print("Warmup started\r");
      nodes.stream().parallel().forEach(Node::warmup);
      System.out.print("Warmup finished.\r\n");
   }

   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder builder) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.serialization().addContextInitializers(SerializationCtxBuilder.INSTANCE);
      GlobalConfiguration globalConfiguration = gcb.defaultCacheName("default").build();
      DefaultCacheManager defaultCacheManager = new DefaultCacheManager(globalConfiguration, builder.build(), true);
      cacheManagers.add(defaultCacheManager);
      return defaultCacheManager;
   }


}
