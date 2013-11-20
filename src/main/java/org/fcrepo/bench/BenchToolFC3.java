/**
 *
 */

package org.fcrepo.bench;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import org.uncommons.maths.random.XORShiftRNG;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.http.HttpHost;
import org.apache.http.client.AuthCache;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.protocol.BasicHttpContext;

/**
 * Fedora 3 Benchmarking Tool
 * @author frank asseg
 *
 */
public class BenchToolFC3 {

    private static final DecimalFormat FORMATTER = new DecimalFormat("000.00");

    private final DefaultHttpClient client = new DefaultHttpClient();

    private final URI fedoraUri;

    private final OutputStream ingestOut;
    private final BasicHttpContext authContext;

    public BenchToolFC3(String fedoraUri, String user, String pass)
            throws IOException {
        super();
        ingestOut = new FileOutputStream("ingest.log");
        if (fedoraUri.charAt(fedoraUri.length() - 1) == '/') {
            fedoraUri = fedoraUri.substring(0, fedoraUri.length() - 1);
        }
        this.fedoraUri = URI.create(fedoraUri);
        this.client.getCredentialsProvider().setCredentials(
                new AuthScope(this.fedoraUri.getHost(), this.fedoraUri
                        .getPort()),
                new UsernamePasswordCredentials(user, pass));

        // setup authcache to enable pre-emptive auth
        AuthCache authCache = new BasicAuthCache();
        BasicScheme basicAuth = new BasicScheme();
        authCache.put(new HttpHost(this.fedoraUri.getHost(),this.fedoraUri.getPort()), basicAuth);
        this.authContext = new BasicHttpContext();
        authContext.setAttribute(ClientContext.AUTH_CACHE, authCache);
    }

    private String ingestObject(String label) throws Exception {
        HttpPost post =
                new HttpPost(
                        fedoraUri.toASCIIString() +
                                "/objects/new?format=info:fedora/fedora-system:FOXML-1.1&label=" +
                                label);
        HttpResponse resp = client.execute(post,authContext);
        String answer = IOUtils.toString(resp.getEntity().getContent());
        post.releaseConnection();

        if (resp.getStatusLine().getStatusCode() != 201) {
            throw new Exception("Unable to ingest object, fedora returned " +
                    resp.getStatusLine().getStatusCode());
        }
        return answer;
    }

    private void ingestDatastream(String objectId, String label, long size)
            throws Exception {
        HttpPost post = new HttpPost(fedoraUri.toASCIIString() + "/objects/"
                + objectId + "/datastreams/" + label
                + "?versionable=true&controlGroup=M");
        post.setHeader("Content-Type", "application/octet-stream");
        post.setEntity(new InputStreamEntity(new BenchToolInputStream(size),size));
        long start = System.currentTimeMillis();
        HttpResponse resp = client.execute(post,authContext);
        IOUtils.write((System.currentTimeMillis() - start) + "\n", ingestOut);
        post.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 201) {
            throw new Exception("Unable to ingest datastream " + label
                    + " fedora returned " + resp.getStatusLine());
        }
    }

    private void updateDatastream(String objectId, String label, long size)
            throws Exception {
        HttpPut put = new HttpPut(fedoraUri.toASCIIString() + "/objects/"
                + objectId + "/datastreams/" + label
                + "?versionable=true&controlGroup=M");
        put.setHeader("Content-Type", "application/octet-stream");
        put.setEntity(new InputStreamEntity(new BenchToolInputStream(size),size));
        long start = System.currentTimeMillis();
        HttpResponse resp = client.execute(put,authContext);
        IOUtils.write((System.currentTimeMillis() - start) + "\n", ingestOut);
        put.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new Exception("Unable to update datastream " + label
                    + " fedora returned " + resp.getStatusLine());
        }
    }

    private void readDatastream(String objectId, String label)
            throws Exception {
        HttpGet get = new HttpGet(fedoraUri.toASCIIString() + "/objects/"
                + objectId + "/datastreams/" + label);
        long start = System.currentTimeMillis();
        HttpResponse resp = client.execute(get,authContext);
        InputStream in = resp.getEntity().getContent();
		byte[] buf = new byte[8192];
        for ( int read = -1; (read = in.read(buf)) != -1;  ) { }
        IOUtils.write((System.currentTimeMillis() - start) + "\n", ingestOut);
        get.releaseConnection();

        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new Exception("Unable to update datastream " + label
                    + " fedora returned " + resp.getStatusLine());
        }
    }

    private void deleteObject(String objectId) throws Exception {
        HttpDelete del = new HttpDelete(fedoraUri.toASCIIString() + "/objects/"
                + objectId );
        long start = System.currentTimeMillis();
        HttpResponse resp = client.execute(del,authContext);
        IOUtils.write((System.currentTimeMillis() - start) + "\n", ingestOut);
        del.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new Exception("Unable to delete object fedora returned "
                    + resp.getStatusLine());
        }
    }

    private List<String> listObjects(int numObjects) throws Exception {
        String uri = fedoraUri.toASCIIString() + "/objects?terms=test*"
                + "&resultFormat=xml&pid=true&maxResults="+numObjects;
        HttpGet get = new HttpGet(uri);
        HttpResponse resp = client.execute(get,authContext);

        List<String> pids = new ArrayList<String>();
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse( resp.getEntity().getContent() );
        NodeList nodeList = document.getElementsByTagName("pid");
        for ( int i = 0; i < nodeList.getLength() && i < numObjects; i++ ) {
            Node n = nodeList.item(i);
            pids.add( n.getTextContent() );
        }
        get.releaseConnection();
        return pids;
    }

    private void shutdown() {
        IOUtils.closeQuietly(ingestOut);
    }

    public static void main(String[] args) {
        String uri = args[0];
        String user = args[1];
        String pass = args[2];
        int numObjects = Integer.parseInt(args[3]);
        long size = Integer.parseInt(args[4]);
        String action = "ingest";
        if ( args.length > 5 ) { action = args[5]; }
        BenchToolFC3 bench = null;
        if ( action != null && action.equals("delete") ) {
            System.out.println("deleting " + numObjects + " objects");
        } else if ( action != null && action.equals("update") ) {
            System.out.println("updating " + numObjects
                    + " objects with datastream size " + size);
        } else if ( action != null && action.equals("read") ) {
            System.out.println("reading " + numObjects
                    + " objects with datastream size " + size);
        } else {
            System.out.println("ingesting " + numObjects
                    + " objects with datastream size " + size);
        }
        XORShiftRNG rnd = new XORShiftRNG();
        List<String> pids = null;
        long start = System.currentTimeMillis();
        try {
            bench = new BenchToolFC3(uri, user, pass);
            for (int i = 0; i < numObjects; i++) {
                String objectId = null;
                if ( action != null && (action.equals("delete") || action.equals("update") || action.equals("read")) ) {
                    if ( pids == null ) {
                        pids = bench.listObjects(numObjects);
                    }
                    objectId = pids.get( rnd.nextInt(pids.size()) );
                    if ( action.equals("delete") ) {
                        pids.remove(objectId);
                        bench.deleteObject(objectId);
                    } else if ( action.equals("read") ) {
                        bench.readDatastream(objectId, "ds-1");
                    } else {
                        bench.updateDatastream(objectId, "ds-1", size);
                    }
                } else {
                    objectId = bench.ingestObject("test-" +i);
                    bench.ingestDatastream(objectId, "ds-1", size);
                }
                float percent = (float) (i + 1) / (float) numObjects * 100f;
                System.out.print("\r" + FORMATTER.format(percent) + "%");
            }
            long duration = System.currentTimeMillis() - start;
            System.out.println(" - " + action + " finished");
            System.out.println("Complete " + action + " of " + numObjects + " files took " + duration + " ms\n");
            System.out.println("throughput was  " + FORMATTER.format((double) numObjects * (double) size /1024d / duration) + " mb/s\n");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            bench.shutdown();
        }

    }
}
