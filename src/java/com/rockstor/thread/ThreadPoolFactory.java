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

package com.rockstor.thread;

import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.rockstor.state.StateEnum;
import com.rockstor.util.RockConfiguration;

public class ThreadPoolFactory {
    private static Logger LOG = Logger.getLogger(ThreadPoolFactory.class);
    private static ThreadPoolFactory instance = null;
    private TreeMap<StateEnum, ThreadPool> threadPoolMap = new TreeMap<StateEnum, ThreadPool>();
    private Configuration conf = null;

    public static ThreadPoolFactory getInstance() {
        if (instance == null) {
            instance = new ThreadPoolFactory();
        }
        return instance;
    }

    private ThreadPoolFactory() {
        conf = RockConfiguration.getDefault();

        addThreadPool(StateEnum.READ_HTTP, "rockstor.threads.num.httpReader");
        addThreadPool(StateEnum.WRITE_HTTP, "rockstor.threads.num.httpWriter");
        addThreadPool(StateEnum.READ_META, "rockstor.threads.num.metaReader");
        addThreadPool(StateEnum.WRITE_META, "rockstor.threads.num.metaWriter");
        addThreadPool(StateEnum.READ_CHUNK, "rockstor.threads.num.chunkReader");
        addThreadPool(StateEnum.WRITE_CHUNK, "rockstor.threads.num.chunkWriter");
        addThreadPool(StateEnum.TIMEOUT, "rockstor.threads.num.timeout");
    }

    private void addThreadPool(StateEnum stateCode, String confItem) {
        int threadNum = conf.getInt(confItem, 1);
        if (threadNum < 1) {
            LOG.fatal("configuration error, " + confItem + " less than 1.");
            System.exit(-1);
        }
        ThreadPool threadPool = new ThreadPool(threadNum, stateCode);
        threadPoolMap.put(stateCode, threadPool);
        conf.setInt(confItem, threadNum);
        LOG.info(String.format(
                "ThreadPool %s contains %d threads, Initialized!",
                stateCode.toString(), threadNum));
    }

    public ThreadPool getThreadPool(StateEnum stateCode) {
        return threadPoolMap.get(stateCode);
    }

    public void startAll() {
        for (Entry<StateEnum, ThreadPool> kv : threadPoolMap.entrySet()) {
            kv.getValue().startAll();
        }
    }

    public void stopAll() {
        for (Entry<StateEnum, ThreadPool> kv : threadPoolMap.entrySet()) {
            kv.getValue().stopAll();
        }
    }

    public void joinAll() throws InterruptedException {
        for (Entry<StateEnum, ThreadPool> kv : threadPoolMap.entrySet()) {
            kv.getValue().joinAll();
        }
    }
}
