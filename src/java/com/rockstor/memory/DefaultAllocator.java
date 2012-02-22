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

package com.rockstor.memory;

import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

public class DefaultAllocator implements MemAllocatorInterface,
        DynamicMemoryMXBean {
    private static Logger LOG = Logger.getLogger(DefaultAllocator.class);
    private TreeMap<Integer, LinkedList<ByteBuffer>> memFree = new TreeMap<Integer, LinkedList<ByteBuffer>>();
    private long poolSize = 0;
    private long leftSize = 0; // poolSize - freeSize - usedSize
    private long dropSize = 0;
    private long cachedSize = 0;
    private long usedSize = 0;

    private AtomicLong allocNum = new AtomicLong(0);
    private AtomicLong releaseNum = new AtomicLong(0);
    private AtomicLong failNum = new AtomicLong(0);
    private AtomicLong hitNum = new AtomicLong(0);
    private AtomicLong loseNum = new AtomicLong(0);

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private ReadLock readLock = lock.readLock();
    private WriteLock writeLock = lock.writeLock();

    private int minBufSize = 16 << 10; // limit size 16K.
    private static final String MBEAN_NAME_PREFIX = "com.rockstor.jmx.dynamicMemory:type=";

    public DefaultAllocator() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            String mbean_object_name = MBEAN_NAME_PREFIX + "DefaultAllocator";
            mbs.registerMBean(this, new ObjectName(mbean_object_name));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void init(long poolSize, int minBufSize) {
        assert (poolSize > 0);
        this.poolSize = poolSize;
        leftSize = poolSize;
        this.minBufSize = minBufSize;
        LOG.info("init memory allocator (poolsize=" + readableSpace(poolSize)
                + ", minBufSize=" + readableSpace(minBufSize) + ") OK!");
    }

    public int align(int len) {
        assert (len > 0);
        int alignSize = minBufSize;
        --len;
        len >>= 14;
        while (len > 0) {
            alignSize <<= 1;
            len >>= 1;
        }

        return alignSize;
    }

    public ByteBuffer allocate(int len) {
        allocNum.incrementAndGet();

        ByteBuffer buf = null;

        // can not satisfy
        if (len > poolSize) {
            failNum.incrementAndGet();
            LOG.warn("Alloc Failed: invalid parameter, len=" + len
                    + ", poolSize=" + poolSize + "!");
            return null;
        }

        int rLen = align(len);

        try {
            writeLock.lock();
            // left memory and free memory are all less than request
            if ((leftSize + cachedSize) < rLen) {
                failNum.incrementAndGet();
                LOG.warn("Alloc Failed: no space!");
                return null;
            }

            // reuse free memory first
            if (cachedSize >= rLen) {
                // find from free
                buf = allocFromFree(rLen);
                if (buf != null) {
                    rLen = buf.getDataLen();
                    cachedSize -= rLen;
                    usedSize += rLen;
                    hitNum.incrementAndGet();
                    return buf;
                }
            }

            // if left memory is less than request, release some memory first
            if (leftSize < rLen) {
                free(rLen);

                // no more memory
                if (leftSize < rLen) {
                    failNum.incrementAndGet();
                    LOG.warn("Alloc Failed: no space after free!");
                    return null;
                }
            }

            // allocate new buffer
            return allocNewBuffer(rLen);
        } finally {
            writeLock.unlock();
        }
    }

    public void release(ByteBuffer buf) {
        if (buf == null) {
            return;
        }
        releaseNum.incrementAndGet();
        try {
            writeLock.lock();
            int dataLen = buf.getDataLen();
            usedSize -= dataLen;
            cachedSize += dataLen;

            LinkedList<ByteBuffer> bufList = memFree.get(dataLen);
            if (bufList == null) {
                bufList = new LinkedList<ByteBuffer>();
                memFree.put(dataLen, bufList);
            }

            bufList.add(buf);
            buf.reset();
            LOG.info("Release " + buf);
        } finally {
            writeLock.unlock();
        }
    }

    private ByteBuffer allocNewBuffer(int len) {
        // allocate new buffer
        leftSize -= len;
        usedSize += len;
        loseNum.incrementAndGet();
        ByteBuffer buf = new ByteBuffer(len);
        LOG.info("Alloc New " + buf);
        return buf;
    }

    private ByteBuffer allocFromFree(int len) {
        Entry<Integer, LinkedList<ByteBuffer>> entry = memFree
                .ceilingEntry(len);
        if (entry == null) {
            return null;
        }

        LinkedList<ByteBuffer> list = entry.getValue();
        ByteBuffer buf = list.poll();

        if (list.isEmpty()) {
            memFree.remove(entry.getKey());
        }

        LOG.info("Reuse " + buf);

        return buf;
    }

    // release biggest first
    private void free(int size) {
        Map.Entry<Integer, LinkedList<ByteBuffer>> entry = null;
        LinkedList<ByteBuffer> list = null;
        int bufSize = 0;

        while (size > 0 && (entry = memFree.lastEntry()) != null) {
            list = entry.getValue();
            bufSize = entry.getKey().intValue();
            while (size > 0 && !list.isEmpty()) {
                LOG.info("Free: " + list.poll());
                size -= bufSize;
                leftSize += bufSize;
                cachedSize -= bufSize;
                dropSize += bufSize;
            }

            if (list.isEmpty()) {
                memFree.pollLastEntry();
            }
        }
    }

    @Override
    public long getAllocNum() {
        return allocNum.get();
    }

    @Override
    public long getSuccNum() {
        return hitNum.get() + loseNum.get();
    }

    @Override
    public long getHitNum() {
        return hitNum.get();
    }

    @Override
    public long getLoseNum() {
        return this.loseNum.get();
    }

    @Override
    public long getFailNum() {
        return this.failNum.get();
    }

    @Override
    public long getOutgoingNum() {
        return hitNum.get() + loseNum.get() - releaseNum.get();
    }

    @Override
    public long getReleaseNum() {
        return this.releaseNum.get();
    }

    @Override
    public float getSuccRate() {
        long succNum = hitNum.get() + loseNum.get();
        long alloc1Num = allocNum.get();
        return (alloc1Num > 0) ? (succNum * 100.0f) / alloc1Num : 0.0f;
    }

    @Override
    public float getHitRate() {
        long succNum = hitNum.get() + loseNum.get();
        return (succNum > 0) ? (hitNum.get() * 100.0f) / succNum : 0.0f;
    }

    @Override
    public float getLoseRate() {
        long succNum = hitNum.get() + loseNum.get();
        return (succNum > 0) ? (loseNum.get() * 100.0f) / succNum : 0.0f;
    }

    @Override
    public float getFailRate() {
        long succNum = hitNum.get() + loseNum.get();
        long alloc1Num = allocNum.get();
        return (alloc1Num > 0) ? ((alloc1Num - succNum) * 100.0f) / alloc1Num
                : 0.0f;
    }

    @Override
    public float getReleaseRate() {
        long succNum = hitNum.get() + loseNum.get();
        return (succNum > 0) ? (releaseNum.get() * 100.0f) / succNum : 0.0f;
    }

    @Override
    public float getOutgoingRate() {
        long succNum = hitNum.get() + loseNum.get();
        long outgoingNum = succNum - releaseNum.get();
        return (succNum > 0) ? (outgoingNum * 100.0f) / succNum : 0.0f;
    }

    public String getStatics() {
        try {
            readLock.lock();
            long succNum = hitNum.get() + loseNum.get();
            long alloc1Num = allocNum.get();
            long outgoingNum = succNum - releaseNum.get();
            float succRate = (allocNum.get() > 0) ? (succNum * 100.0f)
                    / alloc1Num : 0.0f;
            float hitRate = succNum > 0 ? (hitNum.get() * 100.0f) / succNum
                    : 0.0f;
            float releaseRate = (succNum > 0) ? (releaseNum.get() * 100.0f)
                    / succNum : 0.0f;
            float loseRate = 100.0f - hitRate;
            float failRate = 100.0f - succRate;
            float outgoingRate = 100.0f - releaseRate;
            return String
                    .format("[statics: <times: alloc=%d, hit=%d, lose=%d, fail=%d, release=%d, outgoing=%d>,"
                            + " <Rate: succRate=%.2f%%, hitRate=%.2f%%, loseRate=%.2f%%, failRate=%.2f%%, releaseRate=%.2f%%, outgoingRate=%.2f%%>]",
                            allocNum.get(), hitNum.get(), loseNum.get(),
                            failNum.get(), releaseNum.get(), outgoingNum,
                            succRate, hitRate, loseRate, failRate, releaseRate,
                            outgoingRate);
        } finally {
            readLock.unlock();
        }
    }

    public static String readableSpace(long len) {
        final long[] rate = new long[] { 1L << 30, 1L << 20, 1L << 10, 1 };
        final String[] desc = new String[] { "GB", "MB", "KB", "B", "B" };
        if (len <= 0) {
            return String.format("0 B");
        }
        int idx = 0;
        for (; idx < rate.length; ++idx) {
            if (len >= rate[idx])
                break;
        }

        return String.format("%.2f %s", (len * 1.0f) / rate[idx], desc[idx]);
    }

    @Override
    public String toString() {
        try {
            readLock.lock();
            float usedRate = (usedSize * 100.0f) / poolSize;
            float leftRate = (leftSize * 100.0f) / poolSize;
            float cachedRate = (cachedSize * 100.0f) / poolSize;
            float dropRate = (dropSize * 100.0f) / poolSize;
            return String
                    .format("[MemManager: <space: used=%s, cached=%s, left=%s, poolSize=%s, dropSize=%s>, "
                            + "<Rate: used=%.2f%%, cached=%.2f%%, left=%.2f%%, drop=%.2f%%>]",
                            readableSpace(usedSize), readableSpace(cachedSize),
                            readableSpace(leftSize), readableSpace(poolSize),
                            readableSpace(dropSize), usedRate, cachedRate,
                            leftRate, dropRate);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getPoolSize() {
        return this.poolSize;
    }

    @Override
    public long getMinBlockSize() {
        return this.minBufSize;
    }

    @Override
    public float getUsedRate() {
        try {
            readLock.lock();
            return (usedSize * 100.0f) / poolSize;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public float getLeftRate() {
        try {
            readLock.lock();
            return (leftSize * 100.0f) / poolSize;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public float getCachedRate() {
        try {
            readLock.lock();
            return (cachedSize * 100.0f) / poolSize;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public float getDroppedRate() {
        try {
            readLock.lock();
            return (this.dropSize * 100.0f) / poolSize;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getUsedSpace() {
        try {
            readLock.lock();
            return this.usedSize;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getLeftSpace() {
        try {
            readLock.lock();
            return this.leftSize;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getCachedSpace() {
        try {
            readLock.lock();
            return this.cachedSize;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getDroppedSpace() {
        try {
            readLock.lock();
            return this.dropSize;
        } finally {
            readLock.unlock();
        }
    }

    public static void main(String[] argv) {
        DefaultAllocator bfm = new DefaultAllocator(); // 1M
        bfm.init(1 << 20, 1 << 14);
        ByteBuffer buf = null;

        buf = bfm.allocate(1 << 19);
        bfm.release(buf);
        buf = bfm.allocate((1 << 19) + 1);

        LOG.info(bfm.getStatics());
        LOG.info(bfm);
    }
}
