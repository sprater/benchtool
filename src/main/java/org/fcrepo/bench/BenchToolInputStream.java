/**
 *
 */
package org.fcrepo.bench;

import java.io.IOException;
import java.io.InputStream;

import java.util.Random;
import org.uncommons.maths.random.XORShiftRNG;


/**
 * @author frank asseg
 *
 */
public class BenchToolInputStream extends InputStream {

    private final long size;
    private long idx = 0;
    private final Random rng;

    public BenchToolInputStream(long size) {
        super();
        this.size = size;

        if ( "java.util.Random".equals(System.getProperty("random.impl")) ) {
            /* standard jdk random */
            rng = new Random();
        } else {
            /* quite the fast RNG from uncommons-math */
            rng = new XORShiftRNG();
        }
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
