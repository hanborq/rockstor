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

package com.rockstor.core.db;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

public class DBJMX implements DBJMXMBean {
    @Override
    public long getDelDelay() { // ms/sec
        long curDelOps = delOps.get();
        long curDelay = delDelay.get();

        long ret = 0;

        if (curDelOps != lastDelOps) {
            ret = (curDelay - lastDelDelay) / (curDelOps - lastDelOps);
            lastDelOps = curDelOps;
            lastDelDelay = curDelay;
        }

        return ret;
    }

    @Override
    public long getDelOps() { // ops/sec
        long curTime = System.currentTimeMillis();
        long curDelOps = delOps.get();

        long ret = 0;

        if (curTime != lastDelOpsThTime) {
            ret = (curDelOps - lastDelOpsTh) / (curTime - lastDelOpsThTime);
            lastDelOpsTh = curDelOps;
            lastDelOpsThTime = curTime;
        }

        return ret;
    }

    @Override
    public long getGetDelay() { // ms/sec
        long curGetOps = getOps.get();
        long curGetDelay = getDelay.get();

        long ret = 0;

        if (curGetOps != lastGetOps) {
            ret = (curGetDelay - lastGetDelay) / (curGetOps - lastGetOps);
            lastGetOps = curGetOps;
            lastGetDelay = curGetDelay;
        }

        return ret;
    }

    @Override
    public long getGetOps() { // ops/sec
        long curTime = System.currentTimeMillis();
        long curGetOps = getOps.get();

        long ret = 0;

        if (curTime != lastGetOpsThTime) {
            ret = (curGetOps - lastGetOpsTh) / (curTime - lastGetOpsThTime);
            lastGetOpsTh = curGetOps;
            lastGetOpsThTime = curTime;
        }

        return ret;
    }

    @Override
    public long getPutDelay() { // ms/sec
        long curPutOps = putOps.get();
        long curPutDelay = putDelay.get();

        long ret = 0;

        if (curPutOps != lastPutOps) {
            ret = (curPutDelay - lastPutDelay) / (curPutOps - lastGetOps);
            lastPutOps = curPutOps;
            lastPutDelay = curPutDelay;
        }

        return ret;
    }

    @Override
    public long getPutOps() { // ops/sec
        long curTime = System.currentTimeMillis();
        long curPutOps = getOps.get();

        long ret = 0;

        if (curTime != lastPutOpsThTime) {
            ret = (curPutOps - lastPutOpsTh) / (curTime - lastPutOpsThTime);
            lastPutOpsTh = curPutOps;
            lastPutOpsThTime = curTime;
        }

        return ret;
    }

    @Override
    public long getScanDelay() { // ms/sec
        long curScanOps = scanOps.get();
        long curScanDelay = scanDelay.get();

        long ret = 0;

        if (curScanOps != lastScanOps) {
            ret = (curScanDelay - lastScanDelay) / (curScanOps - lastGetOps);
            lastScanOps = curScanOps;
            lastScanDelay = curScanDelay;
        }

        return ret;
    }

    @Override
    public long getScanOps() { // ops/sec
        long curTime = System.currentTimeMillis();
        long curScanOps = getOps.get();

        long ret = 0;

        if (curTime != lastScanOpsThTime) {
            ret = (curScanOps - lastScanOpsTh) / (curTime - lastScanOpsThTime);
            lastScanOpsTh = curScanOps;
            lastScanOpsThTime = curTime;
        }

        return ret;
    }

    public void put(long delay) {
        putOps.incrementAndGet();
        putDelay.addAndGet(delay);
    }

    public void get(long delay) {
        getOps.incrementAndGet();
        getDelay.addAndGet(delay);
    }

    public void del(long delay) {
        delOps.incrementAndGet();
        delDelay.addAndGet(delay);
    }

    public void scan(long delay) {
        scanOps.incrementAndGet();
        scanDelay.addAndGet(delay);
    }

    public static Logger LOG = Logger.getLogger(DBJMX.class);
    private static final String MBEAN_OBJECT_NAME_PREFIX = "com.rockstor.core.db:type=";
    private String mbean_name = null;

    private AtomicLong putOps = new AtomicLong(0);
    private AtomicLong getOps = new AtomicLong(0);
    private AtomicLong delOps = new AtomicLong(0);
    private AtomicLong scanOps = new AtomicLong(0);

    private AtomicLong putDelay = new AtomicLong(0);
    private AtomicLong getDelay = new AtomicLong(0);
    private AtomicLong delDelay = new AtomicLong(0);
    private AtomicLong scanDelay = new AtomicLong(0);

    private long lastPutOps = 0;
    private long lastGetOps = 0;
    private long lastDelOps = 0;
    private long lastScanOps = 0;

    private long lastPutDelay = 0;
    private long lastGetDelay = 0;
    private long lastDelDelay = 0;
    private long lastScanDelay = 0;

    private long lastPutOpsTh = 0;
    private long lastGetOpsTh = 0;
    private long lastDelOpsTh = 0;
    private long lastScanOpsTh = 0;

    private long lastPutOpsThTime = System.currentTimeMillis();
    private long lastGetOpsThTime = System.currentTimeMillis();
    private long lastDelOpsThTime = System.currentTimeMillis();
    private long lastScanOpsThTime = System.currentTimeMillis();

    public DBJMX(String tabName) {
        mbean_name = MBEAN_OBJECT_NAME_PREFIX + tabName;
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        LOG.info("Register DBJMXMBean: " + mbean_name);
        try {
            mbs.registerMBean(this, new ObjectName(mbean_name));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
