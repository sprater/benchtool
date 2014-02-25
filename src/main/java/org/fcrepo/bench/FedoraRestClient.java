
package org.fcrepo.bench;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.fcrepo.bench.BenchTool.FedoraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FedoraRestClient {

    private static final Logger LOG = LoggerFactory.getLogger(FedoraRestClient.class);

    private final FedoraVersion version;

    protected final URI fedoraUri;

    // The transaction to be used by applicable operations performed by this client
    protected Transaction tx;

    public FedoraRestClient(final URI fedoraUri, final FedoraVersion version) {
        super();
        this.version = version;
        this.fedoraUri = fedoraUri;
    }

    protected abstract long deleteDatastream(String pid) throws IOException;

    protected abstract long deleteObject(String pid) throws IOException;

    protected abstract long createObject(String pid) throws IOException;

    protected abstract long createDatastream(String pid, long size) throws IOException;

    protected abstract long retrieveDatastream(String pid) throws IOException;

    protected abstract long updateDatastream(String pid, long size) throws IOException;

    protected abstract int getClusterSize() throws IOException;

    final void purgeObjects(final List<String> pids, final boolean removeDatastreams) {
        for (final String pid : pids) {
            try {
                if (removeDatastreams) {
                    this.deleteDatastream(pid);
                }
                this.deleteObject(pid);
            } catch (final IOException e) {
                LOG.error("Unable to purge objects in Fedora", e);
            }
        }
    }

    final long createObjects(final List<String> pids) {
        long duration = 0;
        for (final String pid : pids) {
            try {
                duration += this.createObject(pid);
            } catch (final IOException e) {
                LOG.error("Unable to prepare objects in Fedora", e);
            }
        }
        return duration;
    }

    public void createDatastreams(final List<String> pids, final long size) {
        for (final String pid : pids) {
            try {
                this.createDatastream(pid, size);
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
    protected long createTransaction(final Transaction tx) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Tells the Fedora API to commit the transaction provided
     *
     * @param transaction
     * @return
     * @throws IOException
     */
    protected long commitTransaction(final Transaction transaction) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Tells the Fedora API to rollback the provided transaction
     *
     * @param transaction
     * @return
     * @throws IOException
     */
    protected long rollbackTransaction(final Transaction transaction) throws IOException {
        throw new UnsupportedOperationException();
    }

    public Transaction getTransaction() {
        return this.tx;
    }

    public void setTransaction(final Transaction tx) {
        this.tx = tx;
    }

    public static FedoraRestClient createClient(final URI fedoraUri, final FedoraVersion version) {
        switch (version) {
            case FCREPO3:
                return new Fedora3RestClient(fedoraUri);
            case FCREPO4:
                return new Fedora4RestClient(fedoraUri);
            default:
                throw new IllegalArgumentException("No client available for Fedora Version" + version.name());
        }
    }
}