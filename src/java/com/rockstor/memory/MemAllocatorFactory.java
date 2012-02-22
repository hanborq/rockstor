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

import java.util.LinkedList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.RockConfiguration;

public class MemAllocatorFactory {
    private static Logger LOG = Logger.getLogger(MemAllocatorFactory.class);
    private static MemAllocatorFactory instance = null;
    private MemAllocatorInterface allocator = null;
    private static final String defaultAllocatorClassName = "com.rockstor.memory.DefaultAllocator";
    private static final long defaultPoolSize = 1L << 30; // 1G
    private static final int defaultMinBufSize = 1 << 14; // 16K
    private static final long defaultReservedSize = 128L << 20; // 128M

    /**
     * rockstor.memory.allocator.class com.rockstor.memory.DefaultAllocator
     * rockstor.memory.minBufferSize 16384 16K rockstor.memory.poolSize
     * 1073741824 1G
     */
    private MemAllocatorFactory() {
        Configuration conf = RockConfiguration.getDefault();
        long poolSize = conf.getLong("rockstor.memory.poolSize",
                defaultPoolSize);

        if (poolSize < (1L << 20)) {
            poolSize = (1L << 20);
        }

        long reservedSize = conf.getLong("rockstor.memory.reservedSize",
                defaultReservedSize);
        if (reservedSize < defaultReservedSize) {
            reservedSize = defaultReservedSize;
        }

        long leftSize = Runtime.getRuntime().maxMemory() - reservedSize;

        if (leftSize < 0) {
            LOG.fatal("configuration error, rockstor.memory.reservedSize="
                    + DefaultAllocator.readableSpace(reservedSize)
                    + ", left memory("
                    + DefaultAllocator.readableSpace(leftSize)
                    + ") is less than ZERO");
            System.exit(-1);
        }

        if (leftSize < poolSize) {
            LOG.warn("configuration warning, reduce rockstor.memory.poolSize "
                    + DefaultAllocator.readableSpace(poolSize)
                    + ", to left meory Size "
                    + DefaultAllocator.readableSpace(leftSize));
            poolSize = leftSize;
        }

        int minBufSize = conf.getInt("rockstor.memory.minBufferSize",
                defaultMinBufSize);
        if (minBufSize < 1024 || minBufSize >= poolSize) {
            minBufSize = 1024;
        }

        String className = conf.get("rockstor.memory.allocator.class",
                defaultAllocatorClassName);
        try {
            allocator = (MemAllocatorInterface) Class.forName(className)
                    .newInstance();
        } catch (Exception e) {
            ExceptionLogger.log(LOG, "new memory allocator (" + className
                    + ") instance failed", e);
            System.exit(-1);
        }

        LOG.info("New Memory Allocator Instance (" + className + ") OK!");

        allocator.init(poolSize, minBufSize);
    }

    public static MemAllocatorFactory getInstance() {
        if (instance == null) {
            instance = new MemAllocatorFactory();
        }
        return instance;
    }

    public MemAllocatorInterface getAllocator() {
        return allocator;
    }

    public static int next(Random r, int div) {
        return r.nextInt(div);
    }

    public static void main(String[] argv) {
        MemAllocatorInterface allocator = MemAllocatorFactory.getInstance()
                .getAllocator();
        ByteBuffer buf = null;
        LinkedList<ByteBuffer> bufs = new LinkedList<ByteBuffer>();

        Random r = new Random(System.currentTimeMillis());
        int cycle = 10000;
        int min = 15; //
        int max = 64 << 20; // 4M
        int curSize = 0;
        int reservedLen = 10;
        Runtime rt = Runtime.getRuntime();
        try {
            for (int i = 0; i < cycle; i++) {
                curSize = next(r, max) + min;
                buf = allocator.allocate(curSize);
                if (buf != null) {
                    bufs.add(buf);
                }

                while (bufs.size() > reservedLen) {
                    allocator.release(bufs.poll());
                }
            }
            LOG.info(allocator.getStatics());
            LOG.info(allocator);
            for (ByteBuffer b : bufs) {
                allocator.release(b);
            }

        } finally {
            LOG.info(allocator.getStatics());
            LOG.info(allocator);

            LOG.info(String.format("free=%s, max=%s, total=%s",
                    DefaultAllocator.readableSpace(rt.freeMemory()),
                    DefaultAllocator.readableSpace(rt.maxMemory()),
                    DefaultAllocator.readableSpace(rt.totalMemory())));
        }
    }
}
