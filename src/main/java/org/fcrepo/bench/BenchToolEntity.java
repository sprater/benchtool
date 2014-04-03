/**
 *
 */

package org.fcrepo.bench;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;

/**
 * @author frank asseg
 */
public class BenchToolEntity extends InputStreamEntity {

    public BenchToolEntity(long size, byte[] slice) {
        super(new BenchToolInputStream(size, slice), size, ContentType.APPLICATION_OCTET_STREAM);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.http.entity.InputStreamEntity#isRepeatable()
     */
    @Override
    public boolean isRepeatable() {
        return true;
    }
}
