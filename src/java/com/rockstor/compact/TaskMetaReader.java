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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.rockstor.core.io.RockAccessor;
import com.rockstor.core.meta.Rock;
import com.rockstor.util.MD5HashUtil;

public class TaskMetaReader implements Closeable {
    private static Logger LOG = Logger.getLogger(TaskMetaReader.class);
    private String path;
    private FSDataInputStream input;

    public TaskMetaReader() {
    }

    public void open(String path) throws IOException {
        this.path = path;
        FileSystem dfs = RockAccessor.getFileSystem();
        input = dfs.open(new Path(this.path), 64 << 10);
    }

    public Map<String, byte[]> getRocks() throws IOException {
        Map<String, byte[]> rocks = new TreeMap<String, byte[]>();

        byte[] curBytes = null;
        String curStr = null;
        int readLen = Rock.ROCK_ID_LEN;
        while (true) {
            curBytes = new byte[readLen];
            if (input.read(curBytes) != readLen) {
                break;
            }

            curStr = MD5HashUtil.hexStringFromBytes(curBytes);
            rocks.put(curStr, curBytes);
        }

        return rocks;
    }

    @Override
    public void close() throws IOException {
        if (input != null) {
            input.close();
        }
        path = null;
        input = null;
    }

    public static void main(String[] argv) {
        RockAccessor.connectHDFS();
        FileSystem dfs = RockAccessor.getFileSystem();

        try {
            FileStatus[] fs = dfs.listStatus(new Path("/rockstor/tmp/task"));
            TaskMetaReader rir = null;
            for (FileStatus fx : fs) {
                try {
                    rir = new TaskMetaReader();
                    rir.open(fx.getPath().toString() + "/meta");
                    Map<String, byte[]> s = rir.getRocks();
                    LOG.info(fx.getPath().toString() + "/meta");
                    for (Map.Entry<String, byte[]> kv : s.entrySet()) {
                        LOG.info(kv.getKey());
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
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                dfs.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }
}
