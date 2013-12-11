/**
 *
 */

package org.fcrepo.bench;

import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.protocol.BasicHttpContext;
import org.fcrepo.bench.BenchTool.FedoraVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author frank asseg
 *
 */
public class Fedora4RestClient extends FedoraRestClient {

    private static final Logger LOG = LoggerFactory
            .getLogger(Fedora4RestClient.class);

    public Fedora4RestClient(final URI fedoraUri) {
        super(fedoraUri, FedoraVersion.FCREPO4);
    }

    @Override
    protected long createObject(final String pid) throws IOException {
        final HttpPost post = new HttpPost(this.fedoraUri + "/rest/objects/" + pid);
        final UsernamePasswordCredentials creds =
                BenchTool.getUserCredentials();
        final BasicHttpContext context = new BasicHttpContext();
        try {
            post.addHeader(new BasicScheme().authenticate(creds, post, context));
        } catch (final AuthenticationException ae) {
            LOG.error(ae.getMessage());
        }
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
                this.fedoraUri + "/rest/objects/" + pid + "/ds1/fcr:content";
        final HttpPost post = new HttpPost(dsUri);
        final UsernamePasswordCredentials creds =
                BenchTool.getUserCredentials();
        final BasicHttpContext context = new BasicHttpContext();
        try {
            post.addHeader(new BasicScheme().authenticate(creds, post, context));
        } catch (final AuthenticationException ae) {
            LOG.error(ae.getMessage());
        }
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
                this.fedoraUri + "/rest/objects/" + pid + "/ds1/fcr:content";
        final HttpPut put = new HttpPut(dsUri);
        final UsernamePasswordCredentials creds =
                BenchTool.getUserCredentials();
        final BasicHttpContext context = new BasicHttpContext();
        try {
            put.addHeader(new BasicScheme().authenticate(creds, put, context));
        } catch (final AuthenticationException ae) {
            LOG.error(ae.getMessage());
        }
        put.setEntity(new BenchToolEntity(size, BenchTool.RANDOM_SLICE));
        final long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(put);
        final long duration = System.currentTimeMillis() - start;
        put.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 204) {
            throw new IOException("Unable to update datastream at " + dsUri +
                    "\nFedora returned " + resp.getStatusLine().getStatusCode());
        }
        return duration;
    }

    @Override
    protected long retrieveDatastream(final String pid) throws IOException {
        final String dsUri =
                this.fedoraUri + "/rest/objects/" + pid + "/ds1/fcr:content";
        final HttpGet get = new HttpGet(dsUri);
        final UsernamePasswordCredentials creds =
                BenchTool.getUserCredentials();
        final BasicHttpContext context = new BasicHttpContext();
        try {
            get.addHeader(new BasicScheme().authenticate(creds, get, context));
        } catch (final AuthenticationException ae) {
            LOG.error(ae.getMessage());
        }
        final long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(get);
        final long duration = System.currentTimeMillis() - start;
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
                new HttpDelete(this.fedoraUri + "/rest/objects/" + pid);
        final UsernamePasswordCredentials creds =
                BenchTool.getUserCredentials();
        final BasicHttpContext context = new BasicHttpContext();
        try {
            delete.addHeader(new BasicScheme().authenticate(creds, delete,
                    context));
        } catch (final AuthenticationException ae) {
            LOG.error(ae.getMessage());
        }
        final long time = System.currentTimeMillis();
        BenchTool.httpClient.execute(delete);
        final long duration = System.currentTimeMillis() - time;
        delete.releaseConnection();
        return duration;
    }

    @Override
    protected long deleteDatastream(final String pid) throws IOException {
        final String dsUri = this.fedoraUri + "/rest/objects/" + pid + "/ds1";
        final HttpDelete delete = new HttpDelete(dsUri);
        final UsernamePasswordCredentials creds =
                BenchTool.getUserCredentials();
        final BasicHttpContext context = new BasicHttpContext();
        try {
            delete.addHeader(new BasicScheme().authenticate(creds, delete,
                    context));
        } catch (final AuthenticationException ae) {
            LOG.error(ae.getMessage());
        }
        final long start = System.currentTimeMillis();
        final HttpResponse resp = BenchTool.httpClient.execute(delete);
        final long duration = System.currentTimeMillis() - start;
        delete.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 204) {
            throw new IOException("Unable to delete datastream from " + dsUri +
                    "\nFedora returned " + resp.getStatusLine().getStatusCode());
        }
        return duration;
    }
}
