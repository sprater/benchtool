/**
 *
 */

package org.fcrepo.bench;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.fcrepo.bench.BenchTool.Action;
import org.fcrepo.bench.BenchTool.FedoraVersion;
import org.fcrepo.bench.TransactionManager.TransactionMode;
import static org.fcrepo.bench.TransactionManager.TransactionMode.ROLLBACK;
import static org.fcrepo.bench.TransactionManager.TransactionMode.COMMIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.icu.text.DecimalFormat;

public class FCRepoBenchRunner {

    private static final DecimalFormat FORMAT = new DecimalFormat("###.##");

    private static final Logger LOG = LoggerFactory
            .getLogger(FCRepoBenchRunner.class);

    private final List<BenchToolResult> results = Collections
            .synchronizedList(new ArrayList<BenchToolResult>());

    private final FedoraVersion version;

    private final URI fedoraUri;

    private final Action action;

    private final int numBinaries;

    private final long size;

    private final int numThreads;

    private final ExecutorService executor;

    private final FedoraRestClient fedora;

    private FileOutputStream logOut;

    private final TransactionManager txManager;

    private final TransactionManager prepTxManager;

    private long runTime;

    public FCRepoBenchRunner(final FedoraVersion version, final URI fedoraUri,
            final Action action, final int numBinaries, final long size, final int numThreads,
            final String logpath, final TransactionMode txMode, final int actionsPerTx, final int parallelTx,
            final boolean preparationAsTx) throws IOException {
        super();
        this.version = version;
        this.fedoraUri = fedoraUri;
        this.action = action;
        this.numBinaries = numBinaries;
        this.size = size;
        this.numThreads = numThreads;
        this.executor = Executors.newFixedThreadPool(numThreads);
        this.fedora = FedoraRestClient.createClient(fedoraUri, version);

        if (txMode == TransactionMode.NONE || version == FedoraVersion.FCREPO3) {
            if (txMode != TransactionMode.NONE) {
                LOG.warn("Transactions are not supported by this version of Fedora, transaction settings ignored");
            }
            this.txManager = null;
        } else {
            this.txManager = new TransactionManager(txMode, actionsPerTx, parallelTx);
            LOG.debug("Transactions enabled in mode " + txMode + " with " + actionsPerTx
                    + " actions per tx, " + parallelTx + " parallel tx");

        }
        if (preparationAsTx && version != FedoraVersion.FCREPO3) {
            prepTxManager = new TransactionManager(COMMIT, 0, 1);
        } else {
            prepTxManager = null;
        }

        try {
            this.logOut = new FileOutputStream(logpath);
        } catch (final FileNotFoundException e) {
            this.logOut = null;
            LOG.warn(
                    "Unable to open log file at {}. No log output will be generated",
                    logpath);
        }
    }

    public void runBenchmark() throws IOException{
        runTime = System.currentTimeMillis();

        this.logParameters();
        /*
         * first create the required top level objects so their creation won't
         * affect the pure action performance
         */
        final List<String> pids = prepareObjects();

        LOG.info("scheduling {} actions", numBinaries);

        /* schedule all the action workers for execution */
        final List<Future<BenchToolResult>> futures;
        if (txManager == null) {
            futures = getActionFutures(pids);
        } else {
            futures = getTransactionalActionFutures(pids);
        }

        /* retrieve the workers' results */
        try {
            this.fetchResults(futures);
        } catch (InterruptedException | ExecutionException | IOException e) {
            LOG.error("Error while getting results from worker threads",e);
        }finally{
            this.executor.shutdown();
        }

        /* delete all the created objects and datastreams from the repository */
        if (txManager == null || txManager.getMode() != ROLLBACK) {
            this.purgeObjects(pids);
        }

        runTime = System.currentTimeMillis() - runTime;

        this.logResults();
    }

    private List<Future<BenchToolResult>> getActionFutures(final List<String> pids) throws IOException {
        final List<Future<BenchToolResult>> futures = new ArrayList<>();

        for (final String pid : pids) {
            futures.add(executor.submit(new ActionWorker(action, fedoraUri, pid, size, version, null)));
        }

        return futures;
    }

    private List<Future<BenchToolResult>> getTransactionalActionFutures(final List<String> pids) throws IOException {
        final List<Future<BenchToolResult>> futures = new ArrayList<>();

        for (final String pid : pids) {
            final Transaction tx = txManager.getTransaction();

            // Create the transaction if it has not been initialized yet
            if (!tx.actionsAssigned()) {
                LOG.debug("Adding create tx worker");
                futures.add(executor.submit(new ActionWorker(Action.CREATE_TX,
                        fedoraUri, null, 0, version, tx)));
            }
            tx.assignAction();

            futures.add(executor.submit(new ActionWorker(action, fedoraUri, pid, size, version, tx)));

            // Finalize the transaction if it is complete
            if (tx.allActionsAssigned()) {
                futures.add(executor.submit(new ActionWorker(txManager.getFinalizeAction(),
                        fedoraUri, null, 0, version, tx)));
            }
        }

        // Finalize any lingering incomplete transactions
        for (final Transaction tx : txManager.getTransactions()) {
            if (!tx.allActionsAssigned()) {
                tx.setMaxActions(tx.getActionsAssigned());
                futures.add(executor.submit(new ActionWorker(txManager.getFinalizeAction(),
                        fedoraUri, null, 0, version, tx)));
            }
        }

        return futures;
    }

    private void logParameters() throws IOException {
        LOG.info(
                "Running {} {} action(s) against {} with a binary size of {} using {} thread(s)",
                new Object[] {numBinaries, action.name(), version.name(), convertSize(size),
                        numThreads});
        if (version == FedoraVersion.FCREPO4) {
            LOG.info("The Fedora cluster has {} node(s) before the benchmark",
                    this.fedora.getClusterSize());
        }
    }

    private void logResults() throws IOException {
        long duration = 0;
        long numBytes = 0;
        for (final BenchToolResult res : results) {
            duration = duration + res.getDuration();
            numBytes = numBytes + res.getSize();
        }
        float throughputPerThread = 0f;
        throughputPerThread = size * numBinaries * 1000f / (1024f * 1024f * duration);

        /* now the bench is finished and the result will be printed out */
        LOG.info("Completed {} {} action(s) executed in {} ms {}",
                new Object[] { this.numBinaries, action, duration,
                txManager == null? "" : "(includes tx create/commit)"});

        if (version == FedoraVersion.FCREPO4) {
            LOG.info("The Fedora cluster has {} node(s) after the benchmark",
                    this.fedora.getClusterSize());
        }
        if (numThreads == 1) {
            LOG.info("Throughput was {} MB/sec", FORMAT
                    .format(throughputPerThread));
        } else {
            LOG.info("Throughput was {} MB/sec", FORMAT
                    .format(throughputPerThread * numThreads));
            LOG.info("Throughput per thread was {} MB/sec", FORMAT
                    .format(throughputPerThread));
        }

        if (txManager != null) {
            LOG.info("Time spent creating transactions {}ms", txManager.getCreateTime());
            LOG.info("Time spent committing transactions {}ms", txManager.getCommitTime());
            LOG.info("Condensed results:");
            LOG.info("{} {} {} {} {} {} {} {} {} {} {}", new Object[] {numBinaries, size, numThreads, action, duration, throughputPerThread,
                    "tx", txManager.getActionsPerTx(), txManager.getParallelTx(), txManager.getCreateTime(),
                    txManager.getCommitTime()});
        } else {
            LOG.info("Condensed results:");
            LOG.info("{} {} {} {} {} {} {}", new Object[] {numBinaries, size, numThreads, action, duration, throughputPerThread,
                    "no-tx"});
        }

        LOG.info("All operations completed in {} ms", runTime);

    }

    private List<BenchToolResult> fetchResults(final List<Future<BenchToolResult>> futures) throws InterruptedException, ExecutionException, IOException {
        int count = 0;
        for (final Future<BenchToolResult> f : futures) {
                final BenchToolResult res = f.get();
                LOG.debug("{} of {} actions finished", ++count, numBinaries);
                if (logOut != null) {
                    logOut.write((res.getDuration() + "\n").getBytes());
                }
                results.add(res);
        }
        return results;
    }

    private void purgeObjects(final List<String> pids) throws IOException {
        LOG.info("purging {} objects and datastreams", numBinaries);

        startPreparationTx();

        fedora.purgeObjects(pids,action != Action.DELETE);

        commitPreparationTx();
    }

    private List<String> prepareObjects() throws IOException {
        final List<String> pids = new ArrayList<>();
        LOG.info("preparing {} objects", numBinaries);
        for (int i = 0; i < numBinaries; i++) {
            pids.add(UUID.randomUUID().toString());
        }

        startPreparationTx();

        final long duration = fedora.createObjects(pids);
        LOG.info("creating {} objects took {} ms", pids.size(), duration);
        if (this.action == Action.UPDATE || this.action == Action.READ || this.action == Action.DELETE) {
            LOG.info("preparing {} datastreams of size {} for {}", new Object[] {numBinaries, convertSize(size), action});
            // add datastreams in preparation which can be manipulated
            fedora.createDatastreams(pids, size);
        }

        commitPreparationTx();

        return pids;
    }

    private void startPreparationTx() throws IOException {
        if (prepTxManager != null) {
            final Transaction tx;
            tx = prepTxManager.getTransaction();
            fedora.createTransaction(tx);
            fedora.setTransaction(tx);
        }
    }

    private void commitPreparationTx() throws IOException {
        if (prepTxManager != null) {
            final Transaction tx = prepTxManager.getTransaction();
            tx.setReadyForCommit(true);
            fedora.commitTransaction(tx);
            prepTxManager.clearTransactions();
        }
    }

    public static String convertSize(final long size) {
        final int unit =  1024;
        if (size < unit) {
            return size + " B";
        }
        final int exp = (int) (Math.log(size) / Math.log(unit));
        final char pre = "KMGTPE".charAt(exp-1);
        return String.format("%.1f %cB", size / Math.pow(unit, exp), pre);
    }
}
