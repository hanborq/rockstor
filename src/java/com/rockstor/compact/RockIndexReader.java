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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.rockstor.core.io.ChunkReader;
import com.rockstor.core.io.RockAccessor;
import com.rockstor.core.meta.Chunk;

public class RockIndexReader extends RockAccessor {
    private static Logger LOG = Logger.getLogger(RockIndexReader.class);

    private FSDataInputStream input;
    private long fileSize = 0;

    public RockIndexReader() throws IOException {
        super();
    }

    public void open(String path) throws IOException {
        this.path = path;
        input = dfs.open(new Path(path), BUF_SIZE);
        FileStatus fileStatus = dfs.getFileStatus(new Path(path));
        fileSize = fileStatus.getLen();
        LOG.info("create RockIndex Reader: " + this.path + ", size: "
                + fileSize);
    }

    public boolean hasNext() throws IOException {
        return (fileSize - getPos()) >= (Chunk.HEADER_LEN + 8);
    }

    public Chunk next() throws IOException {
        return ChunkReader.readIndex(input);
    }

    @Override
    public void close() throws IOException {
        try {
            // RockDB.close(this);
        } catch (Exception e) {
            throw new IOException(e);
        }

        if (input != null) {
            input.close();
        }
        LOG.info("close RockIndex Reader: " + this.path + ", size: " + fileSize);
        rockMagic = null;
        rockID = null;
        input = null;
        path = null;
        ctime = 0;
        writer = null;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        RockAccessor.connectHDFS();
        FileSystem dfs = RockAccessor.getFileSystem();

        try {
            FileStatus[] fs = dfs
                    .listStatus(new Path("/rockstor/tmp/gb_meta/"));
            RockIndexReader rir = null;
            for (FileStatus fx : fs) {
                try {
                    rir = new RockIndexReader();
                    rir.open(fx.getPath().toString());
                    Chunk c = null;
                    while (rir.hasNext()) {
                        c = rir.next();
                        LOG.info(c);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (rir != null) {
                        rir.close();
                        rir = null;
                    }
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                dfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public long getPos() throws IOException {
        if (dfs == null || input == null) {
            LOG.error("dfs == null or output == null");
            throw new IOException("dfs == null || output == null");
        }
        return input.getPos();
    }

}
