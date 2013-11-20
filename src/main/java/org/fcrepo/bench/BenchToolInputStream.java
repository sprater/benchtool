/**
 *
 */
package org.fcrepo.bench;

import java.io.IOException;
import java.io.InputStream;

import org.uncommons.maths.random.XORShiftRNG;


/**
 * @author frank asseg
 *
 */
public class BenchToolInputStream extends InputStream {

    private final long size;
    private long idx = 0;
    /* quite the fast RNG from uncommons-math */
    private final XORShiftRNG rng = new XORShiftRNG();

    public BenchToolInputStream(long size) {
        super();
        this.size = size;
    }

    /* (non-Javadoc)
     * @see java.io.InputStream#read()
     */
    @Override
    public int read() throws IOException {
        if (idx++ <= size) {
            return rng.nextInt();
        }else {
            throw new IOException("Inputstream size limit reached");
        }
    }
}
