package biz.digitalindustry.storage.benchmark;

import biz.digitalindustry.storage.graph.engine.NativeGraphStore;
import biz.digitalindustry.storage.graph.model.GraphNodeRecord;
import biz.digitalindustry.storage.relational.api.Row;
import biz.digitalindustry.storage.relational.api.TableDefinition;
import biz.digitalindustry.storage.relational.engine.NativeRelationalStore;
import biz.digitalindustry.storage.model.BooleanValue;
import biz.digitalindustry.storage.model.LongValue;
import biz.digitalindustry.storage.model.StringValue;
import biz.digitalindustry.storage.schema.FieldDefinition;
import biz.digitalindustry.storage.schema.IndexDefinition;
import biz.digitalindustry.storage.schema.IndexKind;
import biz.digitalindustry.storage.schema.ValueType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class NexumBenchmarkMain {
    private static final BenchmarkProfile DEFAULT_PROFILE = BenchmarkProfile.SMALL;
    private static final TableDefinition BENCHMARK_TABLE = new TableDefinition(
            "benchmark_users",
            "id",
            List.of(
                    new FieldDefinition("id", ValueType.STRING, true, false),
                    new FieldDefinition("name", ValueType.STRING, true, false),
                    new FieldDefinition("age", ValueType.LONG, true, false),
                    new FieldDefinition("active", ValueType.BOOLEAN, true, false)
            ),
            List.of(
                    new IndexDefinition("users_name_idx", IndexKind.NON_UNIQUE, List.of("name")),
                    new IndexDefinition("users_active_idx", IndexKind.NON_UNIQUE, List.of("active")),
                    new IndexDefinition("users_age_range_idx", IndexKind.ORDERED_RANGE, List.of("age"))
            )
    );

    private NexumBenchmarkMain() {
    }

    public static void main(String[] args) throws Exception {
        BenchmarkProfile profile = BenchmarkProfile.from(System.getProperty("benchmarkProfile"));
        List<BenchmarkResult> results = new ArrayList<>();
        results.addAll(benchmarkWriteLatency(profile));
        results.addAll(benchmarkReopenTime(profile));
        results.addAll(benchmarkGraphBulkRead(profile));
        results.addAll(benchmarkRelationalIndexedLookup(profile));
        results.addAll(benchmarkRelationalMixedReadWrite(profile));

        System.out.println("benchmark,scale,iterations,elapsed_ms,notes");
        for (BenchmarkResult result : results) {
            System.out.printf(
                    "%s,%s,%d,%.3f,%s%n",
                    result.name(),
                    result.scale(),
                    result.iterations(),
                    result.elapsedMs(),
                    result.notes()
            );
        }
    }

    private static List<BenchmarkResult> benchmarkWriteLatency(BenchmarkProfile profile) throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();
        int[] datasetSizes = profile.writeDatasetSizes();
        for (int datasetSize : datasetSizes) {
            logProgress("write_latency_after_preload", datasetSize);
            Path dbPath = createBenchmarkPath("write-latency-" + datasetSize);
            NativeRelationalStore store = null;
            try {
                store = new NativeRelationalStore(dbPath.toString());
                store.registerTable(BENCHMARK_TABLE);
                preloadRows(store, datasetSize);

                int iterations = profile.writeIterations();
                long started = System.nanoTime();
                for (int i = 0; i < iterations; i++) {
                    int rowNumber = datasetSize + i;
                    store.upsert(BENCHMARK_TABLE, userRow(rowNumber));
                }
                results.add(new BenchmarkResult(
                        "write_latency_after_preload",
                        Integer.toString(datasetSize),
                        iterations,
                        nanosToMillis(System.nanoTime() - started),
                        "relational upsert path"
                ));
            } catch (RuntimeException e) {
                results.add(failedResult("write_latency_after_preload", datasetSize, e));
            } finally {
                closeQuietly(store);
                deleteStoreFiles(dbPath);
            }
        }
        return results;
    }

    private static List<BenchmarkResult> benchmarkReopenTime(BenchmarkProfile profile) throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();
        int[] datasetSizes = profile.reopenDatasetSizes();
        for (int datasetSize : datasetSizes) {
            logProgress("reopen_time", datasetSize);
            Path dbPath = createBenchmarkPath("reopen-" + datasetSize);
            NativeRelationalStore store = null;
            try {
                store = new NativeRelationalStore(dbPath.toString());
                store.registerTable(BENCHMARK_TABLE);
                preloadRows(store, datasetSize);
                store.close();
                store = null;

                long started = System.nanoTime();
                store = new NativeRelationalStore(dbPath.toString());
                store.registerTable(BENCHMARK_TABLE);
                results.add(new BenchmarkResult(
                        "reopen_time",
                        Integer.toString(datasetSize),
                        1,
                        nanosToMillis(System.nanoTime() - started),
                        "includes store open and table registration"
                ));
            } catch (RuntimeException e) {
                results.add(failedResult("reopen_time", datasetSize, e));
            } finally {
                closeQuietly(store);
                deleteStoreFiles(dbPath);
            }
        }
        return results;
    }

    private static List<BenchmarkResult> benchmarkGraphBulkRead(BenchmarkProfile profile) throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();
        int[] edgeCounts = profile.graphEdgeCounts();
        int nodeCount = profile.graphNodeCount();
        for (int edgeCount : edgeCounts) {
            logProgress("graph_get_all_nodes", edgeCount);
            Path dbPath = createBenchmarkPath("graph-read-" + edgeCount);
            NativeGraphStore store = null;
            try {
                store = new NativeGraphStore(dbPath.toString());
                for (int i = 0; i < nodeCount; i++) {
                    store.upsertNode("n" + i, "Person");
                }
                for (int i = 0; i < edgeCount; i++) {
                    int from = i % nodeCount;
                    int to = (i * 17 + 7) % nodeCount;
                    store.connectNodes("n" + from, null, "n" + to, null, "KNOWS", 1.0d);
                }

                int iterations = profile.graphIterations();
                long started = System.nanoTime();
                int observedNodeCount = 0;
                for (int i = 0; i < iterations; i++) {
                    List<GraphNodeRecord> nodes = store.getAllNodes();
                    observedNodeCount = nodes.size();
                }
                results.add(new BenchmarkResult(
                        "graph_get_all_nodes",
                        Integer.toString(edgeCount),
                        iterations,
                        nanosToMillis(System.nanoTime() - started),
                        "node_count=" + observedNodeCount
                ));
            } catch (RuntimeException e) {
                results.add(failedResult("graph_get_all_nodes", edgeCount, e));
            } finally {
                closeQuietly(store);
                deleteStoreFiles(dbPath);
            }
        }
        return results;
    }

    private static List<BenchmarkResult> benchmarkRelationalIndexedLookup(BenchmarkProfile profile) throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();
        int[] datasetSizes = profile.lookupDatasetSizes();
        for (int datasetSize : datasetSizes) {
            logProgress("relational_exact_lookup", datasetSize);
            Path dbPath = createBenchmarkPath("indexed-lookup-" + datasetSize);
            NativeRelationalStore store = null;
            try {
                store = new NativeRelationalStore(dbPath.toString());
                store.registerTable(BENCHMARK_TABLE);
                preloadRows(store, datasetSize);

                int iterations = profile.lookupIterations();
                long started = System.nanoTime();
                int observedMatches = 0;
                for (int i = 0; i < iterations; i++) {
                    observedMatches += store.findBy(BENCHMARK_TABLE, "name", new StringValue(nameFor(i % datasetSize))).size();
                }
                results.add(new BenchmarkResult(
                        "relational_exact_lookup",
                        Integer.toString(datasetSize),
                        iterations,
                        nanosToMillis(System.nanoTime() - started),
                        "observed_matches=" + observedMatches
                ));
            } catch (RuntimeException e) {
                results.add(failedResult("relational_exact_lookup", datasetSize, e));
            } finally {
                closeQuietly(store);
                deleteStoreFiles(dbPath);
            }
        }
        return results;
    }

    private static List<BenchmarkResult> benchmarkRelationalMixedReadWrite(BenchmarkProfile profile) throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();
        int[] datasetSizes = profile.mixedDatasetSizes();
        for (int datasetSize : datasetSizes) {
            logProgress("relational_mixed_read_write", datasetSize);
            Path dbPath = createBenchmarkPath("mixed-read-write-" + datasetSize);
            NativeRelationalStore store = null;
            ExecutorService executor = null;
            try {
                store = new NativeRelationalStore(dbPath.toString());
                store.registerTable(BENCHMARK_TABLE);
                preloadRows(store, datasetSize);
                final NativeRelationalStore benchmarkStore = store;

                int readerThreads = profile.mixedReaderThreads();
                int readerIterations = profile.mixedReaderIterations();
                int writerIterations = profile.mixedWriterIterations();
                CountDownLatch startGate = new CountDownLatch(1);
                AtomicInteger writeCursor = new AtomicInteger(datasetSize);
                List<Long> readerLatencies = Collections.synchronizedList(new ArrayList<>());
                executor = Executors.newFixedThreadPool(readerThreads + 1);
                List<Future<?>> futures = new ArrayList<>();

                for (int reader = 0; reader < readerThreads; reader++) {
                    final int readerId = reader;
                    futures.add(executor.submit(() -> {
                        awaitStart(startGate);
                        for (int i = 0; i < readerIterations; i++) {
                            long started = System.nanoTime();
                            int key = (readerId * readerIterations + i) % datasetSize;
                            benchmarkStore.get(BENCHMARK_TABLE, "u" + key);
                            readerLatencies.add(System.nanoTime() - started);
                        }
                        return null;
                    }));
                }

                Future<MixedWriteStats> writerFuture = executor.submit(() -> {
                    awaitStart(startGate);
                    long started = System.nanoTime();
                    for (int i = 0; i < writerIterations; i++) {
                        benchmarkStore.upsert(BENCHMARK_TABLE, userRow(writeCursor.getAndIncrement()));
                    }
                    return new MixedWriteStats(writerIterations, System.nanoTime() - started);
                });

                startGate.countDown();

                for (Future<?> future : futures) {
                    future.get(60, TimeUnit.SECONDS);
                }
                MixedWriteStats writeStats = writerFuture.get(60, TimeUnit.SECONDS);

                List<Long> samples = new ArrayList<>(readerLatencies);
                Collections.sort(samples);
                double readerP50Ms = percentileMillis(samples, 0.50d);
                double readerP95Ms = percentileMillis(samples, 0.95d);
                double readerTotalMs = nanosToMillis(sum(samples));
                double writerTotalMs = nanosToMillis(writeStats.elapsedNanos());

                results.add(new BenchmarkResult(
                        "relational_mixed_read_write_reader",
                        Integer.toString(datasetSize),
                        samples.size(),
                        readerTotalMs,
                        "threads=" + readerThreads
                                + ";p50_ms=" + formatDouble(readerP50Ms)
                                + ";p95_ms=" + formatDouble(readerP95Ms)
                ));
                results.add(new BenchmarkResult(
                        "relational_mixed_read_write_writer",
                        Integer.toString(datasetSize),
                        writeStats.iterations(),
                        writerTotalMs,
                        "avg_ms=" + formatDouble(writerTotalMs / writeStats.iterations())
                                + ";reader_threads=" + readerThreads
                ));
            } catch (RuntimeException e) {
                results.add(failedResult("relational_mixed_read_write", datasetSize, e));
            } finally {
                if (executor != null) {
                    executor.shutdownNow();
                }
                closeQuietly(store);
                deleteStoreFiles(dbPath);
            }
        }
        return results;
    }

    private static BenchmarkResult failedResult(String benchmarkName, int scale, RuntimeException exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            message = exception.getClass().getSimpleName();
        }
        return new BenchmarkResult(
                benchmarkName,
                Integer.toString(scale),
                0,
                -1.0d,
                "FAILED: " + sanitize(message)
        );
    }

    private static void preloadRows(NativeRelationalStore store, int count) {
        for (int i = 0; i < count; i++) {
            store.upsert(BENCHMARK_TABLE, userRow(i));
        }
    }

    private static Row userRow(int i) {
        return new Row("u" + i, Map.of(
                "id", new StringValue("u" + i),
                "name", new StringValue(nameFor(i)),
                "age", new LongValue(20L + (i % 50)),
                "active", new BooleanValue((i & 1) == 0)
        ));
    }

    private static String nameFor(int i) {
        return "name-" + (i % 100);
    }

    private static Path createBenchmarkPath(String prefix) throws IOException {
        Path directory = Path.of("build", "benchmarks");
        Files.createDirectories(directory);
        Path path = directory.resolve(prefix + ".dbs");
        deleteStoreFiles(path);
        return path;
    }

    private static void deleteStoreFiles(Path dbPath) throws IOException {
        if (dbPath == null) {
            return;
        }
        Files.deleteIfExists(Path.of(dbPath + ".indexes"));
        Files.deleteIfExists(Path.of(dbPath + ".records"));
        Files.deleteIfExists(Path.of(dbPath + ".wal"));
        Files.deleteIfExists(dbPath);
    }

    private static void closeQuietly(AutoCloseable closeable) throws Exception {
        if (closeable != null) {
            closeable.close();
        }
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0d;
    }

    private static void logProgress(String benchmarkName, int scale) {
        System.err.printf("running %s scale=%d%n", benchmarkName, scale);
    }

    private static void awaitStart(CountDownLatch startGate) {
        try {
            startGate.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Benchmark interrupted while waiting to start", e);
        }
    }

    private static long sum(List<Long> samples) {
        long total = 0L;
        for (Long sample : samples) {
            total += sample;
        }
        return total;
    }

    private static double percentileMillis(List<Long> sortedSamples, double percentile) {
        if (sortedSamples.isEmpty()) {
            return 0.0d;
        }
        int index = (int) Math.ceil(percentile * sortedSamples.size()) - 1;
        index = Math.max(0, Math.min(index, sortedSamples.size() - 1));
        return nanosToMillis(sortedSamples.get(index));
    }

    private static String formatDouble(double value) {
        return String.format("%.3f", value);
    }

    private static String sanitize(String value) {
        return value.replace(',', ';').replace('\n', ' ').replace('\r', ' ');
    }

    private enum BenchmarkProfile {
        SMALL(
                new int[]{100, 250, 500},
                new int[]{100, 250, 500},
                new int[]{500, 1_000, 2_000},
                250,
                new int[]{100, 250, 500, 1_000},
                25,
                10,
                200,
                new int[]{250, 1_000},
                4,
                100,
                50
        ),
        LARGE(
                new int[]{1_000, 5_000, 10_000},
                new int[]{1_000, 5_000, 10_000},
                new int[]{5_000, 10_000, 20_000},
                1_000,
                new int[]{1_000, 5_000, 10_000},
                25,
                10,
                500,
                new int[]{1_000, 10_000},
                8,
                250,
                100
        );

        private final int[] writeDatasetSizes;
        private final int[] reopenDatasetSizes;
        private final int[] graphEdgeCounts;
        private final int graphNodeCount;
        private final int[] lookupDatasetSizes;
        private final int writeIterations;
        private final int graphIterations;
        private final int lookupIterations;
        private final int[] mixedDatasetSizes;
        private final int mixedReaderThreads;
        private final int mixedReaderIterations;
        private final int mixedWriterIterations;

        BenchmarkProfile(
                int[] writeDatasetSizes,
                int[] reopenDatasetSizes,
                int[] graphEdgeCounts,
                int graphNodeCount,
                int[] lookupDatasetSizes,
                int writeIterations,
                int graphIterations,
                int lookupIterations,
                int[] mixedDatasetSizes,
                int mixedReaderThreads,
                int mixedReaderIterations,
                int mixedWriterIterations
        ) {
            this.writeDatasetSizes = writeDatasetSizes;
            this.reopenDatasetSizes = reopenDatasetSizes;
            this.graphEdgeCounts = graphEdgeCounts;
            this.graphNodeCount = graphNodeCount;
            this.lookupDatasetSizes = lookupDatasetSizes;
            this.writeIterations = writeIterations;
            this.graphIterations = graphIterations;
            this.lookupIterations = lookupIterations;
            this.mixedDatasetSizes = mixedDatasetSizes;
            this.mixedReaderThreads = mixedReaderThreads;
            this.mixedReaderIterations = mixedReaderIterations;
            this.mixedWriterIterations = mixedWriterIterations;
        }

        static BenchmarkProfile from(String value) {
            if (value == null || value.isBlank()) {
                return DEFAULT_PROFILE;
            }
            return BenchmarkProfile.valueOf(value.trim().toUpperCase());
        }

        int[] writeDatasetSizes() {
            return writeDatasetSizes;
        }

        int[] reopenDatasetSizes() {
            return reopenDatasetSizes;
        }

        int[] graphEdgeCounts() {
            return graphEdgeCounts;
        }

        int graphNodeCount() {
            return graphNodeCount;
        }

        int[] lookupDatasetSizes() {
            return lookupDatasetSizes;
        }

        int writeIterations() {
            return writeIterations;
        }

        int graphIterations() {
            return graphIterations;
        }

        int lookupIterations() {
            return lookupIterations;
        }

        int[] mixedDatasetSizes() {
            return mixedDatasetSizes;
        }

        int mixedReaderThreads() {
            return mixedReaderThreads;
        }

        int mixedReaderIterations() {
            return mixedReaderIterations;
        }

        int mixedWriterIterations() {
            return mixedWriterIterations;
        }
    }

    private record MixedWriteStats(
            int iterations,
            long elapsedNanos
    ) {
    }

    private record BenchmarkResult(
            String name,
            String scale,
            int iterations,
            double elapsedMs,
            String notes
    ) {
    }
}
