/**
 *
 */

package org.fcrepo.bench;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uncommons.maths.random.XORShiftRNG;

/**
 * @author frank asseg
 *
 */
public class BenchTool {

    private static final Logger LOG = LoggerFactory.getLogger(BenchTool.class);

    public static final XORShiftRNG RNG = new XORShiftRNG();

    public static final byte[] RANDOM_SLICE = new byte[65535];

    static {
        RNG.nextBytes(RANDOM_SLICE);
    }

    /* should be used by all the threads */
    static CloseableHttpClient httpClient;

    enum Action {
        INGEST, READ, UPDATE, DELETE, LIST;
    }

    enum FedoraVersion {
        FCREPO3, FCREPO4;
    }

    public static void main(String[] args) {
        /* setup the command line options */
        Options ops = createOptions();

        /* set the defaults */
        int numBinaries = 1;
        long size = 1024;
        int numThreads = 1;
        Action action = Action.INGEST;
        URI fedoraUri = URI.create("http://localhost:8080");
        String logPath = "durations.log";

        /* and get the individual settings from the command line */
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cli = parser.parse(ops, args);
            if (cli.hasOption("h")) {
                printUsage(ops);
                return;
            }
            if (cli.hasOption("f")) {
                String uri = cli.getOptionValue("f");
                uri = uri.replaceAll("/*$", "");
                fedoraUri = URI.create(uri);
            }
            if (cli.hasOption("n")) {
                numBinaries = Integer.parseInt(cli.getOptionValue("n"));
            }
            if (cli.hasOption("s")) {
                size = getSizeFromArgument(cli.getOptionValue("s"));
            }
            if (cli.hasOption("a")) {
                action = Action.valueOf(cli.getOptionValue("a").toUpperCase());
            }
            if (cli.hasOption("t")) {
                numThreads = Integer.parseInt(cli.getOptionValue("t"));
            }
            if (cli.hasOption("l")) {
                logPath = cli.getOptionValue("l");
            }
            final HttpClientBuilder clientBuilder =
                    HttpClients.custom().setRedirectStrategy(
                            new DefaultRedirectStrategy()).setRetryHandler(
                            new StandardHttpRequestRetryHandler(0, false));
            if (cli.hasOption("u")) {
                BasicCredentialsProvider cred = new BasicCredentialsProvider();
                cred.setCredentials(new AuthScope(fedoraUri.getHost(),
                        fedoraUri.getPort()), new UsernamePasswordCredentials(
                        cli.getOptionValue('u'), cli.getOptionValue('p')));
                clientBuilder.setDefaultCredentialsProvider(cred);

            }
            httpClient = clientBuilder.build();

        } catch (ParseException e) {
            LOG.error("Unable to parse command line", e);
        }

        try {
            /* start the benchmark runner with the given parameters */
            FCRepoBenchRunner runner =
                    new FCRepoBenchRunner(getFedoraVersion(fedoraUri),
                            fedoraUri, action, numBinaries, size, numThreads,
                            logPath);
            runner.runBenchmark();
        } catch (IOException e) {
            LOG.error("Unable to connect to a Fedora instance at {}", fedoraUri);
        }
    }

    private static long getSizeFromArgument(String optionValue) {
        Matcher m = Pattern.compile("^(\\d*)([kKmMgGtT]{0,1})$").matcher(optionValue);
        if (!m.find()) {
            throw new IllegalArgumentException("Size " + optionValue + " could not be parsed");
        }
        final long size = Long.parseLong(m.group(1));
        if (m.groupCount() == 1) {
            return size;
        }
        final char postfix = m.group(2).charAt(0);
        switch (postfix) {
            case 'k':
            case 'K':
                return size * 1024l ;
            case 'm':
            case 'M':
                return size * 1024l * 1024l;
            case 'g':
            case 'G':
                return size * 1024l * 1024l * 1024l;
            case 't':
            case 'T':
                return size * 1024l * 1024l * 1024l * 1024l;
            default:
                return size;
        }
    }

    @SuppressWarnings("static-access")
    private static Options createOptions() {
        Options ops = new Options();
        ops.addOption(OptionBuilder
                .withArgName("fedora-url")
                .withDescription(
                        "The URL of the Fedora instance. The url must include the context path of the webapp. " +
                        "[default=http://localhost:8080]")
                .withLongOpt("fedora-url").hasArg().create('f'));
        ops.addOption(OptionBuilder.withArgName("num-actions").withDescription(
                "The number of actions performed. [default=1]").withLongOpt(
                "num-actions").hasArg().create('n'));
        ops.addOption(OptionBuilder.withArgName("size").withDescription(
                "The size of the individual binaries used. Sizes with a k,m,g or t postfix will be interpreted as kilo-, mega-, giga- and terabyte [default=1024]")
                .withLongOpt("size").hasArg().create('s'));
        ops.addOption(OptionBuilder
                .withArgName("num-threads")
                .withDescription(
                        "The number of threads used for performing all actions. [default=1]")
                .withLongOpt("num-threads").hasArg().create('t'));
        ops.addOption(OptionBuilder.withArgName("user").withDescription(
                "The fedora user name").withLongOpt("user").hasArg()
                .create('u'));
        ops.addOption(OptionBuilder.withArgName("password").withDescription(
                "The user's password").withLongOpt("password").hasArg().create(
                'p'));
        ops.addOption(OptionBuilder
                .withArgName("action")
                .withDescription(
                        "The action to perform. Can be one of ingest, read, update or delete. [default=ingest]")
                .withLongOpt("action").hasArg().create('a'));
        ops.addOption(OptionBuilder
                .withArgName("log")
                .withDescription(
                        "The log file to which the durations will get written. [default=durations.log]")
                .withLongOpt("log").hasArg().create('l'));
        ops.addOption("h", "help", false, "print the help screen");
        return ops;
    }

    private static FedoraVersion getFedoraVersion(URI fedoraUri)
            throws IOException {
        /* try to determine the Fedora Version using a GET */
        final HttpGet get = new HttpGet(fedoraUri);
        final HttpResponse resp = httpClient.execute(get);

        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Unable to find Fedora Server at URI " +
                    fedoraUri);
        }

        /*
         * just check the html response for a characteristic String to determine
         * the fedora version
         */
        String html = EntityUtils.toString(resp.getEntity());
        get.releaseConnection();
        if (html.contains("<meta http-equiv=\"refresh\" content=\"0;url=describe\">") &&
                html.contains("<title>Redirecting...</title>") &&
                html.contains("<a href=\"describe\">Redirecting...</a>")) {
            /* this seems to be a Fedora 3 instance */
            LOG.info("Found Fedora 3 at " + fedoraUri);
            return FedoraVersion.FCREPO3;
        } else if (html
                .contains("<title>Fedora Commons Repository 4.0</title>") &&
                html.contains("You probably want to visit something a little more interesting, such as:") &&
                html.contains("the Fedora REST API endpoint")) {
            /* this seems to be a Fedora 4 instance */
            LOG.info("Found Fedora 4 at " + fedoraUri);
            return FedoraVersion.FCREPO4;
        } else {
            throw new IOException("Unable to determine Fedora version at " +
                    fedoraUri);
        }
    }

    public static void printUsage(Options ops) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("BenchTool", ops);
        System.out.println("\n\nExamples:\n");
        System.out.println(" * Ingest a single 100mb file:\n   ---------------------------");
        System.out.println("   java -jar bench-tool.jar -f http://localhost:8080/fcrepo -n 1 -a ingest -s 100m\n");
        System.out.println(" * Ingest 20 files of 1gb using 5 threads\n   --------------------------------------");
        System.out.println("   java -jar bench-tool.jar -f http://localhost:8080/fcrepo -n 20 -a ingest -s 1g -t 5\n");
    }
}