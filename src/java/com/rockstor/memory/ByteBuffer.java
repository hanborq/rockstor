/**
 * Copyright 2012 Hanborq Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rockstor.memory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

public class ByteBuffer {
    private static Logger LOG = Logger.getLogger(ByteBuffer.class);
    private static AtomicLong ID_S = new AtomicLong(0);
    private long id = ID_S.incrementAndGet();
    private byte[] data = null;
    private int dataLen = 0;
    private int offset = 0;
    private int len = 0;

    public ByteBuffer(int dataLen) {
        assert (dataLen > 0);
        this.dataLen = dataLen;
        this.data = new byte[dataLen];
    }

    public boolean isFull() {
        return offset == len;
    }

    public int leftBytes() {
        return len - offset;
    }

    public void reset() {
        reset(0);
    }

    public void reset(int len) {
        offset = 0;
        this.len = len;
    }

    public int read(InputStream input) throws IOException {
        int readBytes = input.read(data, offset, len - offset);

        if (readBytes > 0) {
            offset += readBytes;
            LOG.info("offset now: " + offset);
        }

        return readBytes;
    }

    public int write(OutputStream output) throws IOException {
        output.write(data, 0, offset);
        return offset;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getDataLen() {
        return this.dataLen;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        if (len > dataLen || len < 1) {
            throw new IndexOutOfBoundsException();
        }
        this.len = len;
    }

    @Override
    public String toString() {
        return String.format("[ByteBuffer #%d: bufLen=%d, offset=%d, len=%d]",
                id, dataLen, offset, len);
    }
}
