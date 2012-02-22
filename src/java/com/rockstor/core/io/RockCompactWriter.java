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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.rockstor.compact.PathUtil;
import com.rockstor.compact.RockIndexWriter;
import com.rockstor.core.db.RockDB;
import com.rockstor.core.meta.Chunk;
import com.rockstor.core.meta.Rock;
import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.Host;
import com.rockstor.util.MD5HashUtil;
import com.rockstor.util.RunMode;
import com.rockstor.webifc.RestServlet;

public class RockCompactWriter extends RockAccessor {
    private static Logger LOG = Logger.getLogger(RockWriter.class);

    private FSDataOutputStream output;
    private RockIndexWriter rockIndexWriter = null;

    public RockCompactWriter() throws IOException {
        super();
    }

    public void create(String taskIdName) throws IOException {
        this.garbageBytes = 0;
        try {
            rockMagic = MD5HashUtil.hashCodeBytes(Host.getHostName() + ":"
                    + Thread.currentThread().getName() + ":"
                    + System.nanoTime() + ":" + new Random(Long.MAX_VALUE));
        } catch (NoSuchAlgorithmException e) {
            ExceptionLogger.log(LOG, e);
        }
        rockID = MD5HashUtil.hexStringFromBytes(rockMagic);
        path = PathUtil.getInstance().getTaskDataPath(taskIdName, rockID);
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

        // create index file
        rockIndexWriter = new RockIndexWriter();
        rockIndexWriter.create(PathUtil.getInstance().getTaskIndexPath(
                taskIdName));
    }

    @Override
    public void close() throws IOException {
        if (output != null) {
            try {
                RockDB.close(this);
            } catch (Exception e) {
                throw new IOException(e);
            }

            output.close();
            // rename
            dfs.rename(new Path(path), new Path(Rock.HADOOP_DATA_HOME + "/"
                    + rockID));
        }

        if (rockIndexWriter != null) {
            rockIndexWriter.close();
        }

        rockMagic = null;
        rockID = null;
        output = null;
        rockIndexWriter = null;
        path = null;
        ctime = 0;
        writer = null;
    }

    @Override
    public long getPos() throws IOException {
        if (dfs == null || output == null) {
            LOG.error("dfs == null or output == null");
            throw new IOException("dfs == null || output == null");
        }
        return output.getPos();
    }

    public void addChunk(Chunk chunk, FSDataInputStream input)
            throws IOException {
        if (dfs == null || output == null) {
            throw new IOException("dfs == null or output == null");
        }

        // DelayTime dt = new DelayTime();
        chunk.setRockID(rockMagic);
        ChunkWriter.writeHeader(chunk, output);

        writeStream(input, output, chunk.getSize());
        output.sync();

        // write index
        rockIndexWriter.addChunk(chunk);

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
