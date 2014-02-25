
package org.fcrepo.bench;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;

import org.fcrepo.bench.BenchTool.Action;
import org.fcrepo.bench.BenchTool.FedoraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActionWorker implements Callable<BenchToolResult> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(ActionWorker.class);

    private final FedoraRestClient fedora;

    private final Transaction tx;

    private final long binarySize;

    private final Action action;

    private final String pid;

    public ActionWorker(final Action action, final URI fedoraUri, final String pid, final long binarySize,
            final FedoraVersion version, final Transaction tx) {
        super();
        this.binarySize = binarySize;
        this.fedora = FedoraRestClient.createClient(fedoraUri, version);
        this.fedora.setTransaction(tx);
        this.action = action;
        this.pid = pid;
        this.tx = tx;
    }

    /*
     * (non-Javadoc)
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public BenchToolResult call() throws Exception {
        LOGGER.debug("Executing action {} as part of tx {}", this.action, this.tx == null? "none" : this.tx.getTransactionId());
        try {
            /* check the action and run the appropriate test */
            switch (this.action) {
                case INGEST:
                    return doIngest();
                case UPDATE:
                    return doUpdate();
                case READ:
                    return doRead();
                case DELETE:
                    return doDelete();
                case CREATE_TX:
                    return doCreateTx();
                case COMMIT_TX:
                    return doCommitTx();
                case ROLLBACK_TX:
                    return doRollbackTx();
                default:
                    throw new IllegalArgumentException("The Action " +
                            action.name() +
                            " is not available in the worker thread");
            }
        } finally {
            if (tx != null) {
                tx.actionCompleted(this.action);
            }
        }
    }

    private BenchToolResult doDelete() throws IOException {
        final long duration = fedora.deleteDatastream(pid);
        return new BenchToolResult(-1f, duration, binarySize);
    }

    private BenchToolResult doRead() throws IOException {
        final long duration = fedora.retrieveDatastream(pid);
        final float tp = binarySize * 1000f / duration;
        return new BenchToolResult(tp, duration, binarySize);
    }

    private BenchToolResult doUpdate() throws IOException {
        final long duration = fedora.updateDatastream(pid,binarySize);
        final float tp = binarySize * 1000f / duration;
        return new BenchToolResult(tp, duration, binarySize);
    }

    private BenchToolResult doIngest() throws IOException {
        final long duration = fedora.createDatastream(pid, binarySize);
        final float tp = binarySize * 1000f / duration;
        return new BenchToolResult(tp, duration, binarySize);
    }

    private BenchToolResult doCreateTx() throws IOException {
        final long duration = fedora.createTransaction(tx);
        tx.getTxManager().addToCreateTime(duration);
        return new BenchToolResult(0, duration, 0);
    }

    private BenchToolResult doCommitTx() throws IOException {
        final long duration = fedora.commitTransaction(tx);
        tx.getTxManager().addToCommitTime(duration);
        return new BenchToolResult(0, duration, 0);
    }

    private BenchToolResult doRollbackTx() throws IOException {
        final long duration = fedora.rollbackTransaction(tx);
        tx.getTxManager().addToCommitTime(duration);
        return new BenchToolResult(0, duration, 0);
    }
}
