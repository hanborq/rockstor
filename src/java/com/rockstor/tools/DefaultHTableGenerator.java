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

package com.rockstor.tools;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import com.rockstor.util.RockConfiguration;

public class DefaultHTableGenerator implements HTableGenInterface {
    private static Configuration conf = RockConfiguration.getDefault();

    @Override
    public Map<String, HTableDescriptor> initTableDescriptor() {
        Map<String, HTableDescriptor> tbDesMap = new TreeMap<String, HTableDescriptor>();
        HTableDescriptor tbDes = null;

        // init parameter
        // Table
        boolean deferredLogFlush = false;
        long maxFileSize = 512L << 20; // 512M
        long memstoreFlushSize = 64L << 20; // 64M

        // column family
        Compression.Algorithm compressType = Compression.Algorithm.NONE;
        Compression.Algorithm compactionCompressType = Compression.Algorithm.NONE;
        boolean inMemory = false;
        boolean blockCache = true;
        int blockSize = 64 << 10; // 64K
        StoreFile.BloomType bloomType = StoreFile.BloomType.ROW;

        // get param from conf
        deferredLogFlush = conf.getBoolean("rockstor.table.deferredLogFlush",
                deferredLogFlush);
        maxFileSize = conf.getLong("rockstor.table.maxFileSize", maxFileSize);
        memstoreFlushSize = conf.getLong("rockstor.table.memstoreFlushSize",
                memstoreFlushSize);

        compressType = conf.getEnum("rockstor.table.cf.compressionType",
                compressType);
        compactionCompressType = conf.getEnum(
                "rockstor.table.cf.compactionCompressType",
                compactionCompressType);
        inMemory = conf.getBoolean("rockstor.table.cf.inMemory", inMemory);
        blockCache = conf
                .getBoolean("rockstor.table.cf.blockCache", blockCache);
        blockSize = conf.getInt("rockstor.table.cf.blockSize", blockSize);
        bloomType = conf.getEnum("rockstor.table.cf.bloomType", bloomType);

        // init cf
        HColumnDescriptor cfM = new HColumnDescriptor("M");
        cfM.setMaxVersions(1);
        cfM.setCompressionType(compressType);
        cfM.setCompactionCompressionType(compactionCompressType);
        cfM.setInMemory(inMemory);
        cfM.setBlockCacheEnabled(blockCache);
        cfM.setBlocksize(blockSize);
        cfM.setTimeToLive(HColumnDescriptor.DEFAULT_TTL);
        cfM.setBloomFilterType(bloomType);

        HColumnDescriptor cfG = new HColumnDescriptor("G");
        cfM.setMaxVersions(1);
        cfM.setCompressionType(compressType);
        cfM.setCompactionCompressionType(compactionCompressType);
        cfM.setInMemory(inMemory);
        cfM.setBlockCacheEnabled(blockCache);
        cfM.setBlocksize(blockSize);
        cfM.setTimeToLive(HColumnDescriptor.DEFAULT_TTL);
        cfM.setBloomFilterType(bloomType);

        // init HColumnDescriptor

        // "Bucket",
        // "Chunk",
        // "GarbageChunk",
        // "MultiPartPebble",
        // "Pebble",
        // "Rock",
        // "User"
        for (String tbName : RockStorFsFormat.tbs) {
            tbDes = new HTableDescriptor(tbName);
            tbDes.addFamily(cfM);
            if (tbName.equals("Rock")) {
                tbDes.addFamily(cfG);
            }
            tbDes.setDeferredLogFlush(deferredLogFlush);
            tbDes.setMaxFileSize(maxFileSize);
            tbDes.setMemStoreFlushSize(memstoreFlushSize);

            tbDesMap.put(tbName, tbDes);
        }

        return tbDesMap;
    }

    public static void main(String[] args) {
        // init parameter
        // Table
        boolean deferredLogFlush = false;
        long maxFileSize = 512L << 20; // 512M
        long memstoreFlushSize = 64L << 20; // 64M

        // column family
        Compression.Algorithm compressType = Compression.Algorithm.NONE;
        Compression.Algorithm compactionCompressType = Compression.Algorithm.NONE;
        boolean inMemory = false;
        boolean blockCache = true;
        int blockSize = 64 << 10; // 64K
        StoreFile.BloomType bloomType = StoreFile.BloomType.ROW;
        System.out.println(conf.getResource("./"));
        System.out.println(conf.getResource("rockstor-default.xml"));
        // get param from conf
        deferredLogFlush = conf.getBoolean("rockstor.table.deferredLogFlush",
                deferredLogFlush);
        maxFileSize = conf.getLong("rockstor.table.maxFileSize", maxFileSize);
        memstoreFlushSize = conf.getLong("rockstor.table.memstoreFlushSize",
                memstoreFlushSize);
        System.out.println(conf.get("rockstor.table.cf.compressionType"));
        compressType = conf.getEnum("rockstor.table.cf.compressionType",
                compressType);
        System.out
                .println(conf.get("rockstor.table.cf.compactionCompressType"));
        compactionCompressType = conf.getEnum(
                "rockstor.table.cf.compactionCompressType",
                compactionCompressType);
        inMemory = conf.getBoolean("rockstor.table.cf.inMemory", inMemory);
        blockCache = conf
                .getBoolean("rockstor.table.cf.blockCache", blockCache);
        blockSize = conf.getInt("rockstor.table.cf.blockSize", blockSize);
        System.out.println(conf.get("rockstor.table.cf.bloomType"));
        bloomType = conf.getEnum("rockstor.table.cf.bloomType", bloomType);

        System.out.println("compressType: " + compressType);
        System.out.println("bloomType: " + bloomType);
    }
}
