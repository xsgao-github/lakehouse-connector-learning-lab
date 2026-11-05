package xsgao.lakehouseconnector.scan;

import xsgao.lakehouseconnector.model.RecordBatch;
import xsgao.lakehouseconnector.planner.TableSplit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Executes an independent operator pipeline for each split using a bounded thread pool.
 */
public class ScanExecutor {
    private final int maxThreads;

    public ScanExecutor() {
        this(Runtime.getRuntime().availableProcessors());
    }

    public ScanExecutor(int maxThreads) {
        this.maxThreads = Math.max(1, maxThreads);
    }

    /**
     * Returns batches in input split order, including batch order within each split.
     */
    public List<RecordBatch> execute(
            List<TableSplit> splits,
            Function<TableSplit, Iterator<RecordBatch>> pipelineFactory) throws Exception {
        if (splits == null || splits.isEmpty()) {
            return Collections.emptyList();
        }

        int poolSize = Math.min(splits.size(), maxThreads);
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);

        try {
            List<Callable<List<RecordBatch>>> tasks = new ArrayList<>();
            for (TableSplit split : splits) {
                // Construct inside the worker because readers and operators may perform I/O or prefetch.
                tasks.add(() -> drainPipeline(pipelineFactory.apply(split)));
            }

            List<Future<List<RecordBatch>>> futures = executor.invokeAll(tasks);
            List<RecordBatch> results = new ArrayList<>();
            for (Future<List<RecordBatch>> future : futures) {
                results.addAll(future.get());
            }
            return results;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        } finally {
            executor.shutdownNow();
        }
    }

    public List<RecordBatch> drainPipeline(Iterator<RecordBatch> scan) {
        List<RecordBatch> results = new ArrayList<>();
        while (scan.hasNext()) {
            results.add(scan.next());
        }
        return results;
    }
}
