/**
 *
 */

package org.fcrepo.bench;

import static org.fcrepo.bench.TransactionStateManager.TransactionMode.COMMIT;
import static org.fcrepo.bench.TransactionStateManager.TransactionMode.ROLLBACK;

import java.io.IOException;
import java.net.URI;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.fcrepo.bench.BenchTool.FedoraVersion;
import org.fcrepo.bench.TransactionStateManager.TransactionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * @author frank asseg
 */
public class Fedora4RestClient extends FedoraRestClient {

    private static final Logger LOG = LoggerFactory.getLogger(Fedora4RestClient.class);

    public Fedora4RestClient(final URI fedoraUri, final TransactionStateManager txManager) {
        super(fedoraUri, FedoraVersion.FCREPO4, txManager);
    }

    private String getFedoraRestUri(final TransactionState tx) {
        if (tx == null) {
            return this.fedoraUri.toString() + "/rest";
        }
        while (!tx.transactionCreated()) {
            try {
                Thread.sleep(10L);
            } catch (final InterruptedException e) {
            }
        }
        return this.fedoraUri.toString() + "/rest/" + tx.getTransactionId();
    }

    @Override
    protected long createObject(final String pid, final TransactionState tx) throws IOException {
        final String objUri = getFedoraRestUri(tx) + "/objects/" + pid;
        final HttpPost post = new HttpPost(objUri);
        final long time = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(post);
        final long duration = System.currentTimeMillis() - time;
        post.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 201) {
            throw new IOException("Unable to create object at /objects/" + pid + "\nFedora returned " +
                    resp.getStatusLine().getStatusCode());
        }

        return duration;
    }

    @Override
    protected long createDatastream(final String pid, final long size, final TransactionState tx) throws IOException {
        final String dsUri = getFedoraRestUri(tx) + "/objects/" + pid + "/ds1/fcr:content";
        LOG.debug("Creating DS {}", dsUri);
        final HttpPost post = new HttpPost(dsUri);
        post.setEntity(new BenchToolEntity(size, BenchTool.RANDOM_SLICE));
        final long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(post);
        final long duration = System.currentTimeMillis() - start;
        if (resp.getStatusLine().getStatusCode() != 201) {
            throw new IOException("Unable to create datastream at " + dsUri + "\nFedora returned " +
                    resp.getStatusLine().getStatusCode());
        }
        post.releaseConnection();
        return duration;
    }

    @Override
    protected long updateDatastream(final String pid, final long size, final TransactionState tx) throws IOException {
        final String dsUri = getFedoraRestUri(tx) + "/objects/" + pid + "/ds1/fcr:content";
        final HttpPut put = new HttpPut(dsUri);
        put.setEntity(new BenchToolEntity(size, BenchTool.RANDOM_SLICE));
        final long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(put);
        final long duration = System.currentTimeMillis() - start;
        put.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 204) {
            throw new IOException("Unable to update datastream at " + dsUri + "\nFedora returned " +
                    resp.getStatusLine().getStatusCode());
        }
        return duration;
    }

    @Override
    protected long retrieveDatastream(final String pid, final TransactionState tx) throws IOException {
        final String dsUri = getFedoraRestUri(tx) + "/objects/" + pid + "/ds1/fcr:content";
        final HttpGet get = new HttpGet(dsUri);
        final long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(get);
        final long duration = System.currentTimeMillis() - start;
        get.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Unable to retrieve datastream from " + dsUri + "\nFedora returned " +
                    resp.getStatusLine().getStatusCode());
        }
        return duration;
    }

    @Override
    protected long deleteObject(final String pid, final TransactionState tx) throws IOException {
        final HttpDelete delete = new HttpDelete(getFedoraRestUri(tx) + "/objects/" + pid);
        final long time = System.currentTimeMillis();
        BenchTool.httpClient.execute(delete);
        final long duration = System.currentTimeMillis() - time;
        delete.releaseConnection();
        return duration;
    }

    @Override
    protected long deleteDatastream(final String pid, final TransactionState tx) throws IOException {
        final String dsUri = getFedoraRestUri(tx) + "/objects/" + pid + "/ds1";
        final HttpDelete delete = new HttpDelete(dsUri);
        final long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(delete);
        final long duration = System.currentTimeMillis() - start;
        delete.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 204) {
            throw new IOException("Unable to delete datastream from " + dsUri + "\nFedora returned " +
                    resp.getStatusLine().getStatusCode());
        }
        return duration;
    }

    @Override
    protected int getClusterSize() throws IOException {
        final HttpGet get = new HttpGet(this.fedoraUri + "/rest/");
        get.addHeader("Accept", "application/rdf+xml");
        final HttpResponse resp = BenchTool.httpClient.execute(get);
        final Model model = ModelFactory.createDefaultModel();
        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Unable to get cluster size from " + get.getURI() + "\nFedora returned: " +
                    resp.getStatusLine().getStatusCode());
        }
        model.read(resp.getEntity().getContent(), null);
        final StmtIterator it =
                model.listStatements(model.createResource(fedoraUri + "/rest/"), model
                        .createProperty("http://fedora.info/definitions/v4/repository#clusterSize"), (RDFNode) null);
        if (!it.hasNext()) {
            return 0;
        }
        return Integer.parseInt(it.next().getObject().asLiteral().getString());

    }

    @Override
    protected long createTransaction(final TransactionState tx) throws IOException {
        final String txUri = this.fedoraUri + "/rest/fcr:tx";
        final HttpPost post = new HttpPost(txUri);

        final long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(post);
        final long duration = System.currentTimeMillis() - start;
        post.releaseConnection();

        if (resp.getStatusLine().getStatusCode() != 201) {
            throw new IOException("Unable to create transaction." + "\nFedora returned " +
                    resp.getStatusLine().getStatusCode());
        }

        final Header[] locations = resp.getHeaders("Location");
        final String location = locations[0].getValue();

        tx.setTransactionId(location.substring(location.lastIndexOf('/') + 1));

        return duration;
    }

    @Override
    protected long commitTransaction(final TransactionState transaction) throws IOException {
        return finishTransaction(transaction, COMMIT);
    }

    @Override
    protected long rollbackTransaction(final TransactionState transaction) throws IOException {
        return finishTransaction(transaction, ROLLBACK);
    }

    private long finishTransaction(final TransactionState transaction, final TransactionMode mode) throws IOException {

        // Wait for all actions assigned to this transaction to complete for multi-threaded runs
        try {
            while (!transaction.isReadyForCommit()) {
                Thread.sleep(5);
            }
        } catch (final InterruptedException e) {
        }

        String txUri = this.fedoraUri + "/rest/" + transaction.getTransactionId();
        if (mode.equals(COMMIT)) {
            txUri += "/fcr:tx/fcr:commit";
        } else {
            txUri += "/fcr:tx/fcr:rollback";
        }

        LOG.debug("Finishing tx {}", txUri);

        final HttpPost post = new HttpPost(txUri);

        final long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(post);
        final long duration = System.currentTimeMillis() - start;
        post.releaseConnection();

        if (resp.getStatusLine().getStatusCode() != 204) {
            throw new IOException("Failed to " + mode.toString() + " transaction " + transaction.getTransactionId() +
                    "\nFedora returned " + resp.getStatusLine().getStatusCode());
        }

        return duration;
    }

    /* (non-Javadoc)
     * @see org.fcrepo.bench.FedoraRestClient#sparqlUpdate(java.lang.String, org.fcrepo.bench.TransactionState)
     */
    @Override
    protected long sparqlInsert(String pid, TransactionState tx)
            throws IOException {
        String uri = getFedoraRestUri(tx) + "/objects/" + pid;
        String objectUri = this.fedoraUri + "/rest/objects/" + pid;
        final HttpPost post = new HttpPost(uri);
        post.addHeader("Content-Type", "application/sparql-update");
        final String query = "INSERT { <" + objectUri + "> <http://purl.org/dc/elements/1.1/title> \"" + pid + "\" } WHERE {}";
        post.setEntity(new StringEntity(query));
        long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(post);
        long duration = System.currentTimeMillis() - start;
        if (resp.getStatusLine().getStatusCode() != 201)  {
            throw new IOException("Failed to update SPARQL with " + pid + "");
        }
        post.releaseConnection();
        return duration;
    }

    /* (non-Javadoc)
     * @see org.fcrepo.bench.FedoraRestClient#sparqlSelect(java.lang.String, org.fcrepo.bench.TransactionState)
     */
    @Override
    protected long sparqlSelect(String pid, TransactionState tx)
            throws IOException {
        String sparqlUri =this.fedoraUri + "/rest/fcr:sparql";
        final HttpPost post = new HttpPost(sparqlUri);
        final String query = "SELECT ?s WHERE {?s <http://purl.org/dc/elements/1.1/title> \"" + pid + "\"}";
        post.addHeader("Content-Type", "application/sparql-query");
        post.setEntity(new StringEntity(query));
        long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(post);
        long duration = System.currentTimeMillis() - start;
        if (resp.getStatusLine().getStatusCode() != 200)  {
            System.out.println(resp.getStatusLine().getStatusCode());
            throw new IOException("Failed to select SPARQL with " + query);
        }
        post.releaseConnection();
        return duration;
    }

}
