import org.garret.perst.*;

import java.util.concurrent.*;

public class BenchmarkConcurrent {
    static class Record extends Persistent {
        int intKey;
    };

    static class Indices extends Persistent {
        Index intIndex;
    }

    final static int nRecords = 100000;
    static int pagePoolSize = 64*1024*1024;

    static public void main(String[] args) throws Exception {
        final Storage db = StorageFactory.getInstance().createStorage();
        boolean serializableTransaction = false;
        int nThreads = 4;
        for (int i = 0; i < args.length; i++) {
            if ("inmemory".equals(args[i])) {
                pagePoolSize = Storage.INFINITE_PAGE_POOL;
            } else if ("altbtree".equals(args[i])) {
                db.setProperty("perst.alternative.btree", Boolean.TRUE);
                db.setProperty("perst.object.cache.init.size", new Integer(1013));
            } else if ("serializable".equals(args[i])) {
                db.setProperty("perst.alternative.btree", Boolean.TRUE);
                serializableTransaction = true;
            } else if ("gc".equals(args[i])) {
                db.setProperty("perst.gc.threshold", new Integer(1024*1024));
                db.setProperty("perst.background.gc", Boolean.TRUE);
            } else {
                try {
                    nThreads = Integer.parseInt(args[i]);
                } catch (NumberFormatException x) {
                    System.err.println("Unrecognized option: " + args[i]);
                }
            }
        }
        if (pagePoolSize == Storage.INFINITE_PAGE_POOL) {
            db.open(new NullFile(), pagePoolSize);
        } else {
            db.open("benchmark.dbs", pagePoolSize);
        }
        if (serializableTransaction) {
            db.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
        }
        Indices root = (Indices)db.getRoot();
        if (root == null) {
            root = new Indices();
            root.intIndex = db.createIndex(int.class, true);
            db.setRoot(root);
        }
        final Index intIndex = root.intIndex;
        final boolean serializable = serializableTransaction;
        final int threads = nThreads;
        final int chunk = nRecords / threads;
        final int rem = nRecords % threads;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        long start = System.nanoTime();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            executor.execute(new Runnable() {
                public void run() {
                    int from = id * chunk;
                    int to = from + chunk + (id == threads-1 ? rem : 0);
                    if (serializable) {
                        db.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
                    }
                    for (int i = from; i < to; i++) {
                        Record rec = new Record();
                        rec.intKey = i;
                        intIndex.put(new Key(rec.intKey), rec);
                    }
                    if (serializable) {
                        db.endThreadTransaction();
                        db.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
                    } else {
                        db.commit();
                    }
                    for (int i = from; i < to; i++) {
                        Record rec = (Record)intIndex.get(new Key(i));
                        Assert.that(rec != null && rec.intKey == i);
                    }
                    if (serializable) {
                        db.endThreadTransaction();
                    }
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        long elapsed = System.nanoTime() - start;
        long totalOps = (long)nRecords * 2;
        double throughput = (double)totalOps / (elapsed / 1e9);
        System.out.println("Total elapsed time: " + (elapsed / 1e6) + " ms");
        System.out.println("Throughput: " + throughput + " ops/sec");
        db.close();
    }
}
