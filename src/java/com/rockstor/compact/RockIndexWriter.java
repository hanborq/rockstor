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

package com.rockstor.compact;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.rockstor.core.io.ChunkWriter;
import com.rockstor.core.io.RockAccessor;
import com.rockstor.core.meta.Chunk;

public class RockIndexWriter extends RockAccessor {
    private static Logger LOG = Logger.getLogger(RockIndexWriter.class);

    private FSDataOutputStream output;
    private long count = 0;
    private long size = 0;

    public RockIndexWriter() throws IOException {
        super();
    }

    public void create(String path) throws IOException {
        this.path = path;
        output = dfs.create(new Path(path));
        LOG.info("create RockIndex Writer: " + this.path);
    }

    public void addChunk(Chunk chunk) throws IOException {
        ChunkWriter.writeIndex(chunk, output);
        ++count;
        size += chunk.getSize();
        LOG.debug("RockWrite: " + this.path + ", add #" + count + " chunk "
                + chunk + ", gb size now: " + size);
    }

    @Override
    public void close() throws IOException {
        try {
            // RockDB.close(this);
        } catch (Exception e) {
            throw new IOException(e);
        }

        if (output != null) {
            output.close();
        }
        LOG.info("close RockIndex Writer: " + this.path);
        rockMagic = null;
        rockID = null;
        output = null;
        path = null;
        ctime = 0;
        writer = null;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

    @Override
    public long getPos() throws IOException {
        if (dfs == null || output == null) {
            LOG.error("dfs == null or output == null");
            throw new IOException("dfs == null || output == null");
        }
        return output.getPos();
    }

}
