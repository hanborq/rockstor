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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.rockstor.core.meta.Rock;
import com.rockstor.util.RockConfiguration;

public abstract class RockAccessor extends Rock implements Closeable {

    private static Logger LOG = Logger.getLogger(RockAccessor.class);

    protected static FileSystem dfs;

    protected String path;
    protected byte[] buf;

    protected static int BUF_SIZE = 64 * 1024;

    public static void connectHDFS() {
        LOG.info(" Init HDFS Client...");
        Configuration config = RockConfiguration.getDefault();
        URI uri = URI.create(conf.get("rockstor.rootdir"));

        int reconnectTime = 0;
        while (reconnectTime < 3) {
            try {
                reconnectTime++;
                dfs = FileSystem.get(uri, config);
                LOG.info(" Init HDFS OK.");
                break;
            } catch (IOException e) {
                LOG.warn(" Init HDFS Error, reconnect time : " + reconnectTime);
            }
        }
        if (reconnectTime == 3) {
            LOG.error(" Init HDFS Error, exit.");
            dfs = null;
            System.exit(-1);
        }
    }

    public static void disconnectHDFS() {
        if (dfs == null) {
            try {
                dfs.close();
                LOG.info("Close HDFS Client OK.");
            } catch (IOException e) {
                LOG.error("Close HDFS Client ERROR : " + e);
            }
        }
    }

    public static FileSystem getFileSystem() {
        return dfs;
    }

    public RockAccessor() throws IOException {
        buf = new byte[BUF_SIZE];
    }

    public abstract long getPos() throws IOException;

    public abstract void close() throws IOException;

}
