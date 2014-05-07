/**
 *
 */

package org.fcrepo.bench;

import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.util.EntityUtils;
import org.fcrepo.bench.BenchTool.FedoraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author frank asseg
 *
 */
public class Fedora3RestClient extends FedoraRestClient {

    private static final Logger LOG = LoggerFactory
            .getLogger(Fedora3RestClient.class);

    public Fedora3RestClient(final URI fedoraUri) {
        super(fedoraUri, FedoraVersion.FCREPO3);
    }

    @Override
    protected long createObject(final String pid) throws IOException {
        final HttpPost post =
                new HttpPost(this.fedoraUri + "/objects/bt:" + pid +
                        "?format=info:fedora/fedora-system:FOXML-1.1&label=" +
                        pid);
        BenchTool.setAuthentication(post);
        final long time = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(post);
        final long duration = System.currentTimeMillis() - time;
        post.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 201) {
            throw new IOException("Unable to create object at /objects/" + pid +
                    "\nFedora returned " + resp.getStatusLine().getStatusCode());
        }
        return duration;
    }

    @Override
    protected long createDatastream(final String pid, final long size) throws IOException {
        final String dsUri =
                this.fedoraUri + "/objects/bt:" + pid +
                "/datastreams/ds1?versionable=true&controlGroup=M";
        final HttpPost post = new HttpPost(dsUri);
        BenchTool.setAuthentication(post);
        post.setEntity(new BenchToolEntity(size, BenchTool.RANDOM_SLICE));
        final long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(post);
        final long duration = System.currentTimeMillis() - start;
        if (resp.getStatusLine().getStatusCode() != 201) {
            throw new IOException("Unable to create datastream at " + dsUri +
                    "\nFedora returned " + resp.getStatusLine().getStatusCode());
        }
        post.releaseConnection();
        return duration;
    }

    @Override
    protected long updateDatastream(final String pid, final long size) throws IOException {
        final String dsUri =
                this.fedoraUri + "/objects/bt:" + pid +
                "/datastreams/ds1?versionable=true&controlGroup=M";
        final HttpPut put = new HttpPut(dsUri);
        BenchTool.setAuthentication(put);
        put.setEntity(new BenchToolEntity(size, BenchTool.RANDOM_SLICE));
        final long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(put);
        final long duration = System.currentTimeMillis() - start;
        put.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Unable to update datastream at " + dsUri +
                    "\nFedora returned " + resp.getStatusLine().getStatusCode());
        }
        return duration;
    }

    @Override
    protected long retrieveDatastream(final String pid) throws IOException {
        final String dsUri =
                this.fedoraUri + "/objects/bt:" + pid +
                "/datastreams/ds1/content";
        final HttpGet get = new HttpGet(dsUri);
        BenchTool.setAuthentication(get);
        final long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(get);
        final long duration = System.currentTimeMillis() - start;
        EntityUtils.consume(resp.getEntity());
        get.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Unable to retrieve datastream from " +
                    dsUri + "\nFedora returned " +
                    resp.getStatusLine().getStatusCode());
        }
        return duration;
    }

    @Override
    protected long deleteObject(final String pid) throws IOException {
        final HttpDelete delete =
                new HttpDelete(this.fedoraUri + "/objects/bt:" + pid);
        BenchTool.setAuthentication(delete);
        final long time = System.currentTimeMillis();
        BenchTool.httpClient.execute(delete);
        final long duration = System.currentTimeMillis() - time;
        delete.releaseConnection();
        return duration;
    }

    @Override
    protected long deleteDatastream(final String pid) throws IOException {
        final String dsUri =
                this.fedoraUri + "/objects/bt:" + pid + "/datastreams/ds1";
        final HttpDelete delete = new HttpDelete(dsUri);
        BenchTool.setAuthentication(delete);
        final long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(delete);
        final long duration = System.currentTimeMillis() - start;
        delete.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Unable to delete datastream from " + dsUri +
                    "\nFedora returned " + resp.getStatusLine().getStatusCode());
        }
        return duration;
    }
}
