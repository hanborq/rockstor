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

package com.rockstor.task;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.rockstor.util.RockConfiguration;

public abstract class Task {
    private static Logger LOG = Logger.getLogger(Task.class);
    private static AtomicInteger ID_S = new AtomicInteger(0);
    private static AtomicLong leftTask = new AtomicLong(0);
    protected final int id = ID_S.incrementAndGet();
    protected String name = null;
    private final String DEFAULT_TASK_PREFIX = "task";

    protected final long initTs = System.currentTimeMillis();
    protected long lastExecTs = initTs;
    private static long defaultExpireDelay = 60000L; // 1 minute
    private long expireTs = 0;
    private boolean finished = false;

    static {
        Configuration conf = RockConfiguration.getDefault();
        defaultExpireDelay = conf.getLong("rockstor.task.timeout",
                defaultExpireDelay);
        if (defaultExpireDelay < 0) {
            LOG.fatal("configuration error, rockstor.task.timeout value "
                    + defaultExpireDelay + " less than 0!");
            System.exit(-1);
        }
    }

    public Task(String namePrefix) {
        name = String.format("task_%s_%d", (namePrefix != null && !namePrefix
                .isEmpty()) ? namePrefix : DEFAULT_TASK_PREFIX, id);
        leftTask.incrementAndGet();
        expireTs = initTs + defaultExpireDelay;
    }

    public void setExpireTime(long ts) {
        expireTs = ts;
    }

    public void setExpireDelay(long deltaTs) {
        expireTs = initTs + deltaTs;
    }

    public void setDelayAfterLastExec(long deltaTs) {
        expireTs = lastExecTs + deltaTs;
    }

    public void delayExpireTs(long ts) {
        expireTs += ts;
    }

    public boolean expired() {
        return System.currentTimeMillis() > expireTs;
    }

    public boolean expired(long ts) {
        return ts > expireTs;
    }

    public static long getLeftTaskNum() {
        return leftTask.get();
    }

    public static void resetTaskNum() {
        leftTask.set(0L);
    }

    public Task() {
        this(null);
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public abstract void exec();

    public abstract void exec(long ts);

    protected void finish() {
        if (!finished) {
            leftTask.decrementAndGet();
            finished = true;
        }
    }

    public void setLastExecTs(long lastExecTs) {
        this.lastExecTs = lastExecTs;
    }

    public long getLastExecTs() {
        return lastExecTs;
    }
}
