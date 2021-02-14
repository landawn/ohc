/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.caffinitas.ohc.linked;

import static org.caffinitas.ohc.linked.Util.HEADER_COMPRESSED;
import static org.caffinitas.ohc.linked.Util.HEADER_COMPRESSED_WRONG;
import static org.caffinitas.ohc.linked.Util.readFully;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.xerial.snappy.Snappy;

final class DecompressingInputChannel implements ReadableByteChannel {
    private final ReadableByteChannel delegate;

    private final long compressedAddress;
    private ByteBuffer compressedBuffer;
    private ByteBuffer decompressedBuffer;
    private boolean closed;

    /**
     * @param delegate       channel to read from
     *
     */
    DecompressingInputChannel(ReadableByteChannel delegate) throws IOException {
        long headerAdr = Uns.allocateIOException(16);
        int bufferSize;
        int maxCLen;
        try {
            ByteBuffer header = Uns.directBufferFor(headerAdr, 0, 16, false);
            if (!readFully(delegate, header)) {
                throw new EOFException("Could not read file header");
            }
            header.flip();
            int magic = header.getInt();
            if (magic == HEADER_COMPRESSED_WRONG) {
                throw new IOException("File from instance with different CPU architecture cannot be loaded");
            }
            if (magic != HEADER_COMPRESSED) {
                throw new IOException("Illegal file header");
            }
            if (header.getInt() != 1) {
                throw new IOException("Illegal file version");
            }
            bufferSize = header.getInt();
            maxCLen = header.getInt();
        } finally {
            Uns.free(headerAdr);
        }

        this.delegate = delegate;
        this.compressedAddress = Uns.allocateIOException(maxCLen + bufferSize);
        this.compressedBuffer = Uns.directBufferFor(compressedAddress, 0L, maxCLen, false);
        this.compressedBuffer.position(compressedBuffer.limit());

        this.decompressedBuffer = Uns.directBufferFor(compressedAddress, maxCLen, bufferSize, false);
        this.decompressedBuffer.position(decompressedBuffer.limit());
    }

    @Override
    public void close() {
        if (compressedBuffer == null) {
            return;
        }

        this.compressedBuffer = null;
        this.decompressedBuffer = null;
        if (!closed) {
            Uns.free(compressedAddress);
        }
        closed = true;
    }

    @Override
    protected void finalize() throws Throwable {
        if (!closed) {
            Uns.free(compressedAddress);
        }
        super.finalize();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int r = decompressedBuffer.remaining();
        if (r == 0) {
            // read length of compressed buffer
            if (readBytes(4) == 0) {
                return -1;
            }
            int cLen = compressedBuffer.getInt(0);

            // read compressed block
            if (readBytes(cLen) != cLen) {
                throw new EOFException("unexpected EOF");
            }

            // decompress
            decompressedBuffer.clear();
            if (!Snappy.isValidCompressedBuffer(compressedBuffer)) {
                throw new IOException("Invalid compressed data");
            }
            r = Snappy.uncompress(compressedBuffer, decompressedBuffer);
        }

        int dstRem = dst.remaining();
        if (dstRem < r) {
            ByteBuffer dDup = decompressedBuffer.duplicate();
            dDup.limit(dDup.position() + dstRem);
            dst.put(dDup);
            decompressedBuffer.position(decompressedBuffer.position() + dstRem);
            return dstRem;
        } else {
            dst.put(decompressedBuffer);
        }

        return r - decompressedBuffer.remaining();
    }

    private int readBytes(int len) throws IOException {
        // read compressed buffer
        compressedBuffer.clear();
        compressedBuffer.limit(len);
        if (!readFully(delegate, compressedBuffer)) {
            return 0;
        }
        compressedBuffer.position(0);
        return len;
    }

    @Override
    public boolean isOpen() {
        return compressedBuffer != null;
    }
}
