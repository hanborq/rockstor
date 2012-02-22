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
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.rockstor.core.db.RockDB;
import com.rockstor.core.meta.Chunk;
import com.rockstor.memory.ByteBuffer;
import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.Host;
import com.rockstor.util.MD5HashUtil;
import com.rockstor.util.RockConfiguration;
import com.rockstor.util.RunMode;
import com.rockstor.webifc.RestServlet;

public class RockWriter extends RockAccessor {
    private static Logger LOG = Logger.getLogger(RockWriter.class);
    private static Configuration conf = RockConfiguration.getDefault();
    public static long ROCK_MAX_SIZE = conf.getLong("rock.max.size.gb", 1L) * 1024 * 1024 * 1024;

    private FSDataOutputStream output = null;

    public RockWriter() throws IOException {
        super();
    }

    public void create() throws IOException {
        createNewRock();
    }

    @Override
    public void close() throws IOException {
        closeCurrentRock();
    }

    @Override
    public long getPos() throws IOException {
        if (dfs == null || output == null) {
            LOG.error("dfs == null or output == null");
            throw new IOException("dfs == null || output == null");
        }
        return output.getPos();
    }

    public void addChunk(Chunk chunk, ByteBuffer buf) throws IOException {
        if (dfs == null || output == null) {
            throw new IOException("dfs == null or output == null");
        }

        if (output.getPos() > ROCK_MAX_SIZE) {
            closeCurrentRock();
            createNewRock();
        }
        // DelayTime dt = new DelayTime();
        chunk.setRockID(rockMagic);
        ChunkWriter.writeHeader(chunk, output);

        buf.write(output);
        output.sync();
        // ioJMX.write(pebble.getSize(), dt.delay());
    }

    public void addChunk(Chunk chunk, FSDataInputStream input)
            throws IOException {
        if (dfs == null || output == null) {
            throw new IOException("dfs == null or output == null");
        }

        if (output.getPos() > ROCK_MAX_SIZE) {
            closeCurrentRock();
            createNewRock();
        }
        // DelayTime dt = new DelayTime();
        chunk.setRockID(rockMagic);
        ChunkWriter.writeHeader(chunk, output);

        writeStream(input, output, chunk.getSize());
        output.sync();
        // ioJMX.write(pebble.getSize(), dt.delay());
    }

    private void writeStream(FSDataInputStream input, FSDataOutputStream out,
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

    public void copyChunk(Chunk chunk, Chunk srcChunk) throws IOException {

        if (dfs == null || output == null) {
            throw new IOException("dfs == null or output == null");
        }

        if (output.getPos() > ROCK_MAX_SIZE) {
            closeCurrentRock();
            createNewRock();
        }

        RockReader reader = RockReaderPool.getInstance().get(
                MD5HashUtil.hexStringFromBytes(srcChunk.getRockID()));
        try {
            chunk.setRockID(rockMagic);
            ChunkWriter.writeHeader(chunk, output);
            reader.readChunk(srcChunk, output);
        } catch (Exception e) {
            ExceptionLogger.log(LOG, e);
            throw new IOException(e);
        } finally {
            RockReaderPool.getInstance().release(reader);
        }
        output.sync();
    }

    private void createNewRock() throws IOException {
        this.garbageBytes = 0;
        try {
            rockMagic = MD5HashUtil.hashCodeBytes(Host.getHostName() + ":"
                    + Thread.currentThread().getName() + ":"
                    + System.nanoTime() + ":" + new Random(Long.MAX_VALUE));
        } catch (NoSuchAlgorithmException e) {
            ExceptionLogger.log(LOG, e);
        }
        rockID = MD5HashUtil.hexStringFromBytes(rockMagic);
        path = HADOOP_DATA_HOME + "/" + rockID;
        output = dfs.create(new Path(path));
        rockVersion = (int) System.currentTimeMillis();
        ctime = System.currentTimeMillis();
        if (RunMode.MODE_ROCKSERVER.equals(System
                .getProperty(RunMode.PROPERTY_RUNMODE))) {
            writer = Host.getHostName() + ":" + RestServlet.getGeneration();
        } else {
            // writer = RunMode.MODE_MASTERSERVER + ":"
            // + Compactor.getGeneration();
        }
        writeHead();
        try {
            RockDB.create(this);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void closeCurrentRock() throws IOException {

        try {
            RockDB.close(this);
        } catch (Exception e) {
            throw new IOException(e);
        }

        if (output != null)
            output.close();
        rockMagic = null;
        rockID = null;
        output = null;
        path = null;
        ctime = 0;
        writer = null;
    }

    private void writeHead() throws IOException {
        if (dfs == null || output == null) {
            throw new IOException("dfs == null or output == null");
        }
        LOG.info("write head: " + this.toString());
        output.writeInt(rockVersion);
        output.write(rockID.getBytes(), 0, 32);
        output.write(rockMagic);
    }

}
