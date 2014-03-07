
package org.fcrepo.bench;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.fcrepo.bench.BenchTool.FedoraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FedoraRestClient {

    private static final Logger LOG = LoggerFactory.getLogger(FedoraRestClient.class);

    protected final FedoraVersion version;

    protected final URI fedoraUri;

    protected final TransactionStateManager txManager;

    public FedoraRestClient(final URI fedoraUri, final FedoraVersion version, final TransactionStateManager txManager) {
        super();
        this.version = version;
        this.fedoraUri = fedoraUri;
        this.txManager = txManager;
    }

    public FedoraVersion getVersion() {
        return version;
    }

    public TransactionStateManager getTxManager() {
        return txManager;
    }

    protected abstract long deleteDatastream(String pid, TransactionState tx) throws IOException;

    protected abstract long deleteObject(String pid, TransactionState tx) throws IOException;

    protected abstract long createObject(String pid, TransactionState tx) throws IOException;

    protected abstract long createDatastream(String pid, long size, TransactionState tx) throws IOException;

    protected abstract long retrieveDatastream(String pid, TransactionState tx) throws IOException;

    protected abstract long updateDatastream(String pid, long size, TransactionState tx) throws IOException;

    protected abstract int getClusterSize() throws IOException;

    /**
     * Calls Fedora's SPARQL endpoint in order to execute an INSERT query
     * @param pid the pid of the object's sparql record
     * @param tx the Transaction to use if any
     * @return the time required to execute the query
     */
    protected long sparqlInsert(String pid, TransactionState tx)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Calls Fedora's SPARQL endpoint in order to execute an SELECT query
     * @param pid the pid of the object's sparql record
     * @param tx the Transaction to use if any
     * @return the time required to execute the query
     */
    protected long sparqlSelect(String pid, TransactionState tx)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    final void purgeObjects(final List<String> pids, final TransactionState tx) {
        for (final String pid : pids) {
            try {
                this.deleteObject(pid, tx);
            } catch (final IOException e) {
                LOG.error("Unable to purge objects in Fedora", e);
            }
        }
    }

    final long createObjects(final List<String> pids, final TransactionState tx) {
        long duration = 0;
        for (final String pid : pids) {
            try {
                duration += this.createObject(pid, tx);
            } catch (final IOException e) {
                LOG.error("Unable to prepare objects in Fedora", e);
            }
        }
        return duration;
    }

    public void createDatastreams(final List<String> pids, final long size, final TransactionState tx) {
        for (final String pid : pids) {
            try {
                this.createDatastream(pid, size, tx);
            } catch (final IOException e) {
                LOG.error("Unable to prepare datastream in Fedora", e);
            }
        }
    }

    /**
     * Calls the Fedora API to create a new transaction.  The provided Transaction
     * object is assigned the transaction ID of the newly created transaction.
     *
     * @param tx
     * @return
     * @throws IOException
     */
    protected long createTransaction(final TransactionState tx) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Tells the Fedora API to commit the transaction provided
     *
     * @param transaction
     * @return
     * @throws IOException
     */
    protected long commitTransaction(final TransactionState transaction) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Tells the Fedora API to rollback the provided transaction
     *
     * @param transaction
     * @return
     * @throws IOException
     */
    protected long rollbackTransaction(final TransactionState transaction) throws IOException {
        throw new UnsupportedOperationException();
    }

    public static FedoraRestClient createClient(final URI fedoraUri, final FedoraVersion version,
            final TransactionStateManager txManager) {
        switch (version) {
            case FCREPO3:
                return new Fedora3RestClient(fedoraUri);
            case FCREPO4:
                return new Fedora4RestClient(fedoraUri, txManager);
            default:
                throw new IllegalArgumentException("No client available for Fedora Version" + version.name());
        }
    }


}