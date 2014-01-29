
package org.fcrepo.bench;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.fcrepo.bench.BenchTool.FedoraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FedoraRestClient {

    private static final Logger LOG =
            LoggerFactory.getLogger(FedoraRestClient.class);

    private final FedoraVersion version;

    protected final URI fedoraUri;

    public FedoraRestClient(URI fedoraUri, FedoraVersion version) {
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

    final void purgeObjects(List<String> pids, boolean removeDatastreams) {
        for (String pid : pids) {
            try {
                if (removeDatastreams) {
                    this.deleteDatastream(pid);;
                }
                this.deleteObject(pid);
            } catch (IOException e) {
                LOG.error("Unable to prepare objects in Fedora", e);
            }
        }
    }

    final void createObjects(List<String> pids) {
        for (String pid : pids) {
            try {
                this.createObject(pid);
            } catch (IOException e) {
                LOG.error("Unable to prepare objects in Fedora", e);
            }
        }
    }

    public void createDatastreams(List<String> pids, long size) {
        for (String pid : pids) {
            try {
                this.createDatastream(pid, size);
            } catch (IOException e) {
                LOG.error("Unable to prepare datastream in Fedora", e);
            }
        }
    }

    public static FedoraRestClient createClient(URI fedoraUri,
            FedoraVersion version) {
        switch (version) {
            case FCREPO3:
                return new Fedora3RestClient(fedoraUri);
            case FCREPO4:
                return new Fedora4RestClient(fedoraUri);
            default:
                throw new IllegalArgumentException(
                        "No client available for Fedora Version" +
                                version.name());
        }
    }

}