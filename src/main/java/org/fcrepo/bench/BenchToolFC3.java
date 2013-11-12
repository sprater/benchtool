/**
 *
 */

package org.fcrepo.bench;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * @author frank asseg
 *
 */
public class BenchToolFC3 {

    private static final DecimalFormat FORMATTER = new DecimalFormat("000.00");

    private final DefaultHttpClient client = new DefaultHttpClient();

    private final URI fedoraUri;

    private final OutputStream ingestOut;

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
    }

    private String ingestObject(String label) throws Exception {
        HttpPost post =
                new HttpPost(
                        fedoraUri.toASCIIString() +
                                "/objects/new?format=info:fedora/fedora-system:FOXML-1.1&label=" +
                                label);
        HttpResponse resp = client.execute(post);
        String answer = IOUtils.toString(resp.getEntity().getContent());
        post.releaseConnection();

        if (resp.getStatusLine().getStatusCode() != 201) {
            throw new Exception("Unable to ingest object, fedora returned " +
                    resp.getStatusLine().getStatusCode());
        }
        return answer;
    }

    private void ingestDatastream(String objectId, String label, int size)
            throws Exception {
        HttpPost post = new HttpPost(fedoraUri.toASCIIString() + "/objects/"
                + objectId + "/datastreams/" + label
                + "?versionable=true&controlGroup=M");
        post.setHeader("Content-Type", "application/octet-stream");
        post.setEntity(new ByteArrayEntity(getRandomBytes(size)));
        long start = System.currentTimeMillis();
        HttpResponse resp = client.execute(post);
        IOUtils.write((System.currentTimeMillis() - start) + "\n", ingestOut);
        post.releaseConnection();
        if (resp.getStatusLine().getStatusCode() != 201) {
            throw new Exception("Unable to ingest datastream " + label
                    + " fedora returned " + resp.getStatusLine());
        }
    }

    private byte[] getRandomBytes(int size) {
        byte[] data = new byte[size];
        Random r = new Random();
        r.nextBytes(data);
        return data;
    }

    private void shutdown() {
        IOUtils.closeQuietly(ingestOut);
    }

    public static void main(String[] args) {
        String uri = args[0];
        String user = args[1];
        String pass = args[2];
        int numDatastreams = Integer.parseInt(args[3]);
        int size = Integer.parseInt(args[4]);
        BenchToolFC3 bench = null;
        System.out.println("generating " + numDatastreams +
                " datastreams with size " + size);
        long start = System.currentTimeMillis();
        try {
            bench = new BenchToolFC3(uri, user, pass);
            for (int i = 0; i < numDatastreams; i++) {
                String objectId = bench.ingestObject("test-" +i);
                bench.ingestDatastream(objectId, "ds-" + (i + 1), size);
                float percent = (float) (i + 1) / (float) numDatastreams * 100f;
                System.out.print("\r" + FORMATTER.format(percent) + "%");
            }
            long duration = System.currentTimeMillis() - start;
            System.out.println(" - ingest datastreams finished");
            System.out.println("Complete ingest of " + numDatastreams + " files took " + duration + " ms\n");
            System.out.println("throughput was  " + FORMATTER.format((double) numDatastreams * (double) size /1024d / duration) + " mb/s\n");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            bench.shutdown();
        }

    }
}
