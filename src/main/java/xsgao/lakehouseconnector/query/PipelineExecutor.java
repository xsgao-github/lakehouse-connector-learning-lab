package xsgao.lakehouseconnector.query;

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
 * Internal engine: executes an independent operator pipeline for each split using a
 * bounded thread pool. Package-private because it is an implementation detail of
 * {@link QueryExecutor}; callers never drive the pool directly.
 *
 * <p>Learning-lab scope: every per-split pipeline is fully materialized before results are
 * returned, so a query buffers all partial results at once. A streaming design would return
 * an ordered iterator over the splits' pipelines (splits stay in input order) so a downstream
 * combine stage could consume partials incrementally without buffering the whole scan.
 */
class PipelineExecutor {
    private final int maxThreads;

    PipelineExecutor() {
        this(Runtime.getRuntime().availableProcessors());
    }

    PipelineExecutor(int maxThreads) {
        this.maxThreads = Math.max(1, maxThreads);
    }

    /**
     * Returns batches in input split order, including batch order within each split.
     */
    List<RecordBatch> execute(
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

    private List<RecordBatch> drainPipeline(Iterator<RecordBatch> scan) {
        List<RecordBatch> results = new ArrayList<>();
        while (scan.hasNext()) {
            results.add(scan.next());
        }
        return results;
    }
}
