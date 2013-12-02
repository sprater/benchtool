/**
 *
 */

package org.fcrepo.bench;

/**
 * @author frank asseg
 *
 */
public class BenchToolResult {

    private final float throughput;

    private final long duration;

    private final long size;

    public BenchToolResult(float throughput, long duration, long size) {
        super();
        this.throughput = throughput;
        this.duration = duration;
        this.size = size;
    }

    /**
     * @return the duration
     */
    public long getDuration() {
        return duration;
    }

    /**
     * @return the throughput
     */
    public float getThroughput() {
        return throughput;
    }

    /**
     * @return the size
     */
    public long getSize() {
        return size;
    }
}
