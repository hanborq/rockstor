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

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

public class IOStatus implements IOStatusMBean {
    public static Logger LOG = Logger.getLogger(IOStatus.class);
    private static final String MBEAN_OBJECT_NAME = "com.rockstor.core.io:type=IOStatus";
    private AtomicLong readBytes;
    private AtomicLong writeBytes;
    private AtomicLong readOps;
    private AtomicLong writeOps;
    private AtomicLong readDelay;
    private AtomicLong writeDelay;

    private long readDelayLast;
    private long readBytesLast;
    private long readOpsLast;
    private long readTimeLast;

    private long readOpsThLast;
    private long readTimeThLast;

    private long writeDelayLast;
    private long writeBytesLast;
    private long writeOpsLast;
    private long writeTimeLast;

    private long writeOpsThLast;
    private long writeTimeThLast;

    private static IOStatus instance = null;

    private IOStatus() {
        readBytes = new AtomicLong(0);
        writeBytes = new AtomicLong(0);
        readOps = new AtomicLong(0);
        writeOps = new AtomicLong(0);
        readDelay = new AtomicLong(0);
        writeDelay = new AtomicLong(0);

        readDelayLast = 0;
        readBytesLast = 0;
        readOpsLast = 0;
        readTimeLast = System.currentTimeMillis();

        readOpsThLast = 0;
        readTimeThLast = System.currentTimeMillis();

        writeDelayLast = 0;
        writeBytesLast = 0;
        writeOpsLast = 0;
        writeTimeLast = System.currentTimeMillis();

        writeOpsThLast = 0;
        writeTimeThLast = System.currentTimeMillis();
    }

    public static IOStatus getInstance() {
        if (instance == null) {
            instance = new IOStatus();
        }
        return instance;
    }

    static {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        LOG.info("Register IOStatusMBean: " + MBEAN_OBJECT_NAME);
        try {
            mbs.registerMBean(getInstance(), new ObjectName(MBEAN_OBJECT_NAME));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void read(long bytes, long delay) {
        readBytes.addAndGet(bytes);
        readDelay.addAndGet(delay);
        readOps.incrementAndGet();
    }

    public void write(long bytes, long delay) {
        writeBytes.addAndGet(bytes);
        writeDelay.addAndGet(delay);
        writeOps.incrementAndGet();
    }

    @Override
    public long getReadDelay() { // ms/op
        long curReadDelay = readDelay.get();
        long curReadOps = readOps.get();

        long ret = 0;

        if (curReadOps != readOpsLast) {
            ret = (curReadDelay - readDelayLast) / (curReadOps - readOpsLast);
            readOpsLast = curReadOps;
            readDelayLast = curReadDelay;
        }

        return ret;
    }

    @Override
    public long getReadRate() { // kb/sec
        long curReadBytes = readBytes.get();
        long curTime = System.currentTimeMillis();

        long ret = 0;

        if (curTime != readTimeLast) {
            ret = ((curReadBytes - readBytesLast) * 1000)
                    / ((curTime - readTimeLast) * 1024);
            readBytesLast = curReadBytes;
            readTimeLast = curTime;
        }

        return ret;
    }

    @Override
    public long getReadThrought() { // ops/sec
        long curReadOps = readOps.get();
        long curTime = System.currentTimeMillis();

        long ret = 0;

        if (curTime != readTimeThLast) {
            ret = ((curReadOps - readOpsThLast) * 1000)
                    / (curTime - readTimeThLast);
            readOpsThLast = curReadOps;
            readTimeThLast = curTime;
        }

        return ret;
    }

    @Override
    public long getWriteDelay() { // ms/op
        long curWriteDelay = writeDelay.get();
        long curWriteOps = writeOps.get();

        long ret = 0;

        if (curWriteOps != writeOpsLast) {
            ret = (curWriteDelay - writeDelayLast)
                    / (curWriteOps - writeOpsLast);
            writeOpsLast = curWriteOps;
            writeDelayLast = curWriteDelay;
        }

        return ret;
    }

    @Override
    public long getWriteRate() { // kb/sec
        long curWriteBytes = writeBytes.get();
        long curTime = System.currentTimeMillis();

        long ret = 0;

        if (curTime != writeTimeLast) {
            ret = ((curWriteBytes - writeBytesLast) * 1000)
                    / ((curTime - writeTimeLast) * 1024);
            writeBytesLast = curWriteBytes;
            writeTimeLast = curTime;
        }

        return ret;
    }

    @Override
    public long getWriteThrought() { // ops/sec
        long curWriteOps = writeOps.get();
        long curTime = System.currentTimeMillis();

        long ret = 0;

        if (curTime != writeTimeThLast) {
            ret = ((curWriteOps - writeOpsThLast) * 1000)
                    / (curTime - writeTimeThLast);
            writeOpsThLast = curWriteOps;
            writeTimeThLast = curTime;
        }

        return ret;
    }
}
