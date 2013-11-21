/**
 *
 */

package org.fcrepo.bench;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * @author frank asseg
 *
 */
public class BenchToolFC4 {

    private static final DecimalFormat FORMATTER = new DecimalFormat("000.00");

    private static int numThreads = 0;

    private static int maxThreads = 15;


    private static final Logger LOG =
            LoggerFactory.getLogger(BenchToolFC4.class);


    private static int getClusterSize(String uri) throws IOException {
        final Model model = ModelFactory.createDefaultModel();
        model.read(uri + "/rest");
        StmtIterator it = model.listStatements(model.createResource(uri + "/rest/") ,model.createProperty("http://fedora.info/definitions/v4/repository#clusterSize"), (RDFNode) null);
        if (!it.hasNext()) {
            return 0;
        }
        return Integer.parseInt(it.next().getObject().asLiteral().getString());

    }

    public static void main(String[] args) {
        String uri = args[0];
        int numDatastreams = Integer.parseInt(args[1]);
        long size = Long.parseLong(args[2]);
        maxThreads = Integer.parseInt(args[3]);
        BenchToolFC4 bench = null;
        LOG.info("generating {} datastreams with size {}", numDatastreams, size);
        FileOutputStream ingestOut = null;
        try {
            final int initialClusterSize = getClusterSize(uri);
            LOG.info("Initial cluster size is {}", initialClusterSize);
            ingestOut = new FileOutputStream("ingest.log");
            long start = System.currentTimeMillis();
            for (int i = 0; i < numDatastreams; i++) {
                while (numThreads >= maxThreads) {
                    Thread.sleep(10);
                }
                Thread t =
                        new Thread(new Ingester(uri, ingestOut, "benchfc4-" +
                                (i + 1) + "." + UUID.randomUUID(), size));
                t.start();
                numThreads++;
                float percent = (float) (i + 1) / (float) numDatastreams * 100f;
//                System.out.print("\r" + FORMATTER.format(percent) + "%");
            }
            while (numThreads > 0) {
                Thread.sleep(100);
            }
            long duration = System.currentTimeMillis() - start;
            System.out.println(" - ingest datastreams finished");
            LOG.info("Ingest of {} files took {} ms", numDatastreams, duration);
            final int endClusterSize = getClusterSize(uri);
            if (initialClusterSize != endClusterSize) {
                LOG.warn("Initial cluster size was {} but the cluster had size {} at the end", initialClusterSize, endClusterSize);
            }
            LOG.info("Throughput was {} mb/s",FORMATTER.format((double) numDatastreams * (double) size /
                            1024d / duration));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(ingestOut);
        }

    }

    private static class Ingester implements Runnable {

        private final DefaultHttpClient client = new DefaultHttpClient();

        private final URI fedoraUri;

        private final OutputStream ingestOut;

        private final long size;

        private final String pid;

        public Ingester(String fedoraUri, OutputStream out, String pid, long size)
                throws IOException {
            super();
            ingestOut = out;
            if (fedoraUri.charAt(fedoraUri.length() - 1) == '/') {
                fedoraUri = fedoraUri.substring(0, fedoraUri.length() - 1);
            }
            this.fedoraUri = URI.create(fedoraUri);
            this.size = size;
            this.pid = pid;
        }

        @Override
        public void run() {
            try {
                this.ingestObject();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void ingestObject() throws Exception {
            HttpPost post =
                    new HttpPost(fedoraUri.toASCIIString() + "/rest/objects/" +
                            pid + "/DS1/fcr:content");
            post.setHeader("Content-Type", "application/octet-stream");
            post.setEntity(new InputStreamEntity(new BenchToolInputStream(size), size));
            long start = System.currentTimeMillis();
            HttpResponse resp = client.execute(post);
            String answer = IOUtils.toString(resp.getEntity().getContent());
            post.releaseConnection();

            if (resp.getStatusLine().getStatusCode() != 201) {
                LOG.error(answer);
                BenchToolFC4.numThreads--;
                throw new Exception(
                        "Unable to ingest object, fedora returned " +
                                resp.getStatusLine().getStatusCode());
            }
            IOUtils.write((System.currentTimeMillis() - start) + "\n",
                    ingestOut);
            BenchToolFC4.numThreads--;
        }
    }
}
