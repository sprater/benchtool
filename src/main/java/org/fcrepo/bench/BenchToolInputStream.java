
package org.fcrepo.bench;

import java.io.IOException;
import java.io.InputStream;

public class BenchToolInputStream extends InputStream {

    private final long size;

    private long bytesRead;

    private final byte[] slice;

    private final int sliceLen;

    private int slicePos;

    public BenchToolInputStream(long size, byte[] slice) {
        super();
        this.size = size;
        this.slice = slice;
        this.sliceLen = slice.length;
    }

    @Override
    public int read() throws IOException {
        if (slicePos == 0 || slicePos == sliceLen - 1) {
            slicePos = BenchTool.RNG.nextInt((int) Math.floor(sliceLen / 2f));
        }
        return slice[slicePos++];
    }

    @Override
    public int read(byte[] b) throws IOException {
        int i = 0;
        for (; i < b.length; ++i) {
            b[i] = (byte) read();
        }
        return i;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int i = 0;
        for (; i < len; ++i) {
            b[i] = (byte) read();
        }
        return i;
    }
}
