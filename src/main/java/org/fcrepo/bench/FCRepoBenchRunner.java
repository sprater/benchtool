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

    public FCRepoBenchRunner(FedoraVersion version, URI fedoraUri,
            Action action, int numBinaries, long size, int numThreads,
            String logpath) {
        super();
        this.version = version;
        this.fedoraUri = fedoraUri;
        this.action = action;
        this.numBinaries = numBinaries;
        this.size = size;
        this.numThreads = numThreads;
        this.executor = Executors.newFixedThreadPool(numThreads);
        this.fedora = FedoraRestClient.createClient(fedoraUri, version);
        try {
            this.logOut = new FileOutputStream(logpath);
        } catch (FileNotFoundException e) {
            this.logOut = null;
            LOG.warn(
                    "Unable to open log file at {}. No log output will be generated",
                    logpath);
        }
    }

    public void runBenchmark() throws IOException{
        this.logParameters();
        /*
         * first create the required top level objects so their creation won't
         * affect the pure action performance
         */
        final List<String> pids = prepareObjects();

        LOG.info("scheduling {} actions", numBinaries);

        /* schedule all the action workers for execution */
        final List<Future<BenchToolResult>> futures = new ArrayList<>();
        for (String pid : pids) {
            futures.add(executor.submit(new ActionWorker(action, fedoraUri, pid, size, version)));
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
        this.purgeObjects(pids);

        this.logResults();
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
        for (BenchToolResult res : results) {
            duration = duration + res.getDuration();
            numBytes = numBytes + res.getSize();
        }
        float throughputPerThread = 0f;
        throughputPerThread = size * numBinaries * 1000f / (1024f * 1024f * duration);

        /* now the bench is finished and the result will be printed out */
        LOG.info("Completed {} {} action(s) executed in {} ms", new Object[] {
                this.numBinaries, action, duration});
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
    }

    private List<BenchToolResult> fetchResults(List<Future<BenchToolResult>> futures) throws InterruptedException, ExecutionException, IOException {
        int count = 0;
        for (Future<BenchToolResult> f : futures) {
                BenchToolResult res = f.get();
                LOG.debug("{} of {} actions finished", ++count, numBinaries);
                if (logOut != null) {
                    logOut.write((res.getDuration() + "\n").getBytes());
                }
                results.add(res);
        }
        return results;
    }

    private void purgeObjects(List<String> pids) {
        LOG.info("purging {} objects and datastreams", numBinaries);
        fedora.purgeObjects(pids,action != Action.DELETE);
    }

    private List<String> prepareObjects() {
        final List<String> pids = new ArrayList<>();
        LOG.info("preparing {} objects", numBinaries);
        for (int i = 0; i < numBinaries; i++) {
            pids.add(UUID.randomUUID().toString());
        }
        final long duration = fedora.createObjects(pids);
        LOG.info("creating {} objects took {} ms", pids.size(), duration);
        if (this.action == Action.UPDATE || this.action == Action.READ || this.action == Action.DELETE) {
            LOG.info("preparing {} datastreams of size {} for {}", new Object[] {numBinaries, convertSize(size), action});
            // add datastreams in preparation which can be manipulated
            fedora.createDatastreams(pids, size);
        }
        return pids;
    }

    public static String convertSize(long size) {
        int unit =  1024;
        if (size < unit) {
            return size + " B";
        }
        int exp = (int) (Math.log(size) / Math.log(unit));
        char pre = "KMGTPE".charAt(exp-1);
        return String.format("%.1f %cB", size / Math.pow(unit, exp), pre);
    }
}
