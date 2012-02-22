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

package com.rockstor.core.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.rockstor.core.meta.Chunk;
import com.rockstor.memory.ByteBuffer;
import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.MD5HashUtil;

public class RockReader extends RockAccessor implements Comparable<RockReader> {

    private static Logger LOG = Logger.getLogger(RockReader.class);

    private FSDataInputStream input;

    private static AtomicInteger ROCK_READER_CR_ID = new AtomicInteger(0);

    private long ts = System.nanoTime();
    private int cr_id = ROCK_READER_CR_ID.incrementAndGet();
    private long hitTimes = 0;
    private long fileSize = 0;

    public RockReader(byte[] rockMagic) throws IOException {
        this();
        this.rockID = MD5HashUtil.hexStringFromBytes(rockMagic);
        this.rockMagic = rockMagic;
        this.path = HADOOP_DATA_HOME + "/" + rockID;
    }

    private RockReader() throws IOException {
        super();
        this.ts = System.nanoTime();
        this.cr_id = ROCK_READER_CR_ID.incrementAndGet();
    }

    public long getTs() {
        return this.ts;
    }

    public int getCrID() {
        return this.cr_id;
    }

    public void hit() {
        this.ts = System.nanoTime();
        hitTimes += 1;
    }

    @Override
    public String toString() {
        return "[RockReader: " + "rockID=" + this.rockID + ",cr_id="
                + this.cr_id + ", ts=" + this.ts + ", hits=" + this.hitTimes
                + "]";
    }

    public void open() throws IOException {
        if (dfs == null) {
            LOG.error("Open FSDataInputStream error : dfs == null");
            throw new IOException("dfs == null");
        }
        input = dfs.open(new Path(path));
        verifyHead();
        FileStatus fileStatus = dfs.getFileStatus(new Path(path));
        fileSize = fileStatus.getLen();
    }

    public void seekg(long pos) throws IOException {
        LOG.info("Seek to pos(" + pos + ") in rock(" + rockID + ")");
        try {
            input.seek(pos);
        } catch (IOException e) {
            LOG.warn("Seek pos(" + pos
                    + ") ERROR, try to reopen and reseek it.");
            close();
            open();
            input.seek(pos);
        }
    }

    public void nextAlign() throws IOException {
        long p = input.getPos();
        int pedding_bytes = (int) (p & 7);
        if (pedding_bytes != 0) {
            input.seek(p + 8 - pedding_bytes);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (input != null)
                input.close();
            input = null;

        } catch (IOException e) {
            LOG.error("close " + this + ", Exception: "
                    + ExceptionLogger.getStack(e));
            throw e;
        }
        LOG.info("close " + this);
    }

    @Override
    public long getPos() throws IOException {
        if (dfs == null || input == null) {
            LOG.error("getPos catch IOException : dfs == null or input == null");
            throw new IOException("dfs == null || input == null");
        }
        LOG.info("Rock(" + rockID + ") pos : " + input.getPos());
        return input.getPos();
    }

    private void verifyHead() throws IOException {
        if (input == null)
            throw new IOException("Verify Rock Head Error : input == null");

        input.seek(0);
        int v = input.readInt();
        byte[] idBytes = new byte[32];
        input.read(idBytes);
        String id = new String(idBytes);
        byte[] magic = new byte[16];
        input.read(magic);

        LOG.info("Verify Head of Rock " + this.toString() + ", file info [id="
                + id + ", magic=" + MD5HashUtil.hexStringFromBytes(magic)
                + ", version=" + String.valueOf(v) + "]");

        if (v != rockVersion || !id.equals(rockID)
                || Bytes.compareTo(magic, rockMagic) != 0)
            throw new IOException("Verify Rock Head Error.");
    }

    public void readChunk(Chunk chunk, ByteBuffer buf) throws IOException {
        if (input == null) {
            throw new IOException("input == null");
        }
        if (chunk == null || buf == null) {
            throw new IOException("chunk == null or buf == null");
        }

        if (buf.getOffset() == 0) {
            seekg(chunk.getOffset());

            Chunk p = null;
            try {
                p = ChunkReader.readHeader(input);
            } catch (IOException e) {
                close();
                open();
                seekg(chunk.getOffset());
                p = ChunkReader.readHeader(input);
            }
            if (!chunk.equals(p)) {
                throw new IOException("Verify chunk head catch Error Head : "
                        + p + ", it should be : " + chunk);
            }
        } else {

        }

        buf.read(input);
    }

    public void readChunk(Chunk chunk, OutputStream output) throws IOException {
        if (input == null) {
            throw new IOException("input == null");
        }
        if (chunk == null || output == null) {
            throw new IOException("chunk == null or output == null");
        }

        seekg(chunk.getOffset());

        Chunk p = null;
        try {
            p = ChunkReader.readHeader(input);
        } catch (IOException e) {
            close();
            open();
            seekg(chunk.getOffset());
            p = ChunkReader.readHeader(input);
        }
        if (!chunk.equals(p)) {
            throw new IOException("Verify chunk head catch Error Head : " + p
                    + ", it should be : " + chunk);
        }
        writeStream(input, output, p.getSize());
    }

    public boolean hasNext() throws IOException {
        return fileSize - getPos() > Chunk.HEADER_LEN;
    }

    public Chunk nextChunk() {

        Chunk p = null;
        try {
            p = ChunkReader.readHeader(input);
        } catch (Exception e) {
            LOG.warn("Read Chunk Head catch Exception : " + e);
        }
        return p;
    }

    public FSDataInputStream getFSDataInputStream() {
        return input;
    }

    private void writeStream(FSDataInputStream in, OutputStream output,
            long size) throws IOException {

        long remain = size;
        int len = 0;
        int readLen = 0;
        while (remain > 0) {
            readLen = (remain > BUF_SIZE) ? BUF_SIZE : (int) remain;
            len = input.read(buf, 0, readLen);
            output.write(buf, 0, len);
            remain -= len;
        }
    }

    @Override
    public int compareTo(RockReader o) {
        int ret = (int) (this.ts - o.ts);
        if (ret != 0)
            return ret;

        return this.cr_id - o.cr_id;
    }

    @Override
    public boolean equals(Object o) {
        // if(o==this) return true;
        if (o instanceof RockReader) {
            RockReader r = (RockReader) o;
            return this.cr_id == r.cr_id;
        }

        return false;
    }
}
