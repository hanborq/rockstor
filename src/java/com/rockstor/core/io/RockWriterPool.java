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
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.RockConfiguration;

public class RockWriterPool implements RockAccessorPool<RockWriter> {

    private static Logger LOG = Logger.getLogger(RockWriterPool.class);

    private static RockWriterPool instance;
    private LinkedBlockingDeque<RockWriter> writerQueue = new LinkedBlockingDeque<RockWriter>();;

    public static synchronized RockWriterPool getInstance() {
        if (instance == null)
            instance = new RockWriterPool();
        return instance;
    }

    private RockWriterPool() {
        init();
    }

    @Override
    public void init() {
        // writerQueue = new LinkedBlockingDeque<RockWriter>();
        Configuration conf = RockConfiguration.getDefault();
        int initWriteNum = conf.getInt("rockstor.threads.num.chunkWriter", 1);

        for (int idx = 0; idx < initWriteNum; ++idx) {
            createWriter();
        }
    }

    private RockWriter createWriter() {
        RockWriter writer = null;
        try {
            writer = new RockWriter();
            writer.create();
            writerQueue.offer(writer);
        } catch (IOException e) {
            LOG.error("Init RockWriter Error : " + ExceptionLogger.getStack(e));
            if (writer != null)
                try {
                    writer.close();
                } catch (IOException e1) {
                    ExceptionLogger.log(LOG, e1);
                }
        }
        return writer;
    }

    @Override
    public void close() {
        while (!writerQueue.isEmpty()) {
            RockWriter writer = writerQueue.poll();
            try {
                LOG.info("Closing RockWriter " + writer.getRockID() + "...");
                writer.close();
                LOG.info("Close RockWriter OK.");
            } catch (IOException e) {
                LOG.info("Close RockWriter catch IOException : "
                        + ExceptionLogger.getStack(e));
            }
        }
    }

    @Override
    public RockWriter get() {
        RockWriter writer = writerQueue.pollFirst();
        if (writer == null) {
            writer = createWriter();
        }

        return writer;
    }

    @Override
    public void release(RockWriter accessor) {
        writerQueue.offerLast(accessor);
    }

    @Override
    public void remove(RockWriter accessor) {
        writerQueue.remove(accessor);
    }

    @Override
    public int size() {
        return writerQueue.size();
    }

    @Override
    public RockWriter get(String rock) {
        return null;
    }

}
