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

package com.rockstor.core.db.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Pair;

import com.rockstor.core.db.ChunkDB;
import com.rockstor.core.db.PebbleDB;
import com.rockstor.core.io.RockAccessor;
import com.rockstor.core.io.RockReader;
import com.rockstor.core.io.RockReaderPool;
import com.rockstor.core.meta.Chunk;
import com.rockstor.core.meta.Pebble;
import com.rockstor.util.HBaseClient;
import com.rockstor.util.MD5HashUtil;

public class TestDB {
    public static void printBucketRet(
            Pair<ArrayList<Pebble>, ArrayList<String>> bucketRet) {
        System.out.println("------------------------------------");
        if (bucketRet == null) {
            System.out.println("Empty Result!");
        } else {
            for (Pebble p : bucketRet.getFirst()) {
                System.out.println(p);
            }
            System.out.println("---");
            for (String s : bucketRet.getSecond()) {
                System.out.println(s);
            }
            System.out.println("~~~");
            System.out.println("Pebble Num: " + bucketRet.getFirst().size()
                    + ", Common Prefix Num: " + bucketRet.getSecond().size());
        }
        System.out.println("------------------------------------");
        System.out.println();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("Start");
        Configuration conf = HBaseConfiguration.create();
        // conf.set("hbase.master", "192.168.137.15:60000");
        conf.set("hbase.zookeeper.quorum", "192.168.137.15");

        conf.set("hbase.zookeeper.property.clientPort", "2181");
        System.out.println("Begin to init HBaseClient ok!");
        HBaseClient.init(conf, 10);
        System.out.println("HBaseClient init ok!");

        try {
            /*
             * Pair<ArrayList<Pebble>, ArrayList<String>> bucketRet = null;
             * Bucket bucket = new Bucket("bucket","terry");
             * BucketDB.create(bucket);
             * 
             * Pebble pebble = null; Pebble get = null;
             * 
             * pebble = new Pebble("bucket/obj1", now);
             * pebble.setOwner("terry"); pebble.setMIME("txt/plain");
             * pebble.setChunkPrefix
             * (MD5HashUtil.hashCodeBytes(String.valueOf(now)));
             * PebbleDB.put(pebble); System.out.println(pebble); get =
             * PebbleDB.get(pebble.getPebbleID()); System.out.println(get);
             * 
             * pebble = new Pebble("bucket/obj2", now);
             * pebble.setOwner("terry"); pebble.setMIME("txt/plain");
             * pebble.setChunkPrefix
             * (MD5HashUtil.hashCodeBytes(String.valueOf(now)));
             * PebbleDB.put(pebble); System.out.println(pebble); get =
             * PebbleDB.get(pebble.getPebbleID()); System.out.println(get);
             * //PebbleDB.remove(pebble.getPebbleID());
             * 
             * 
             * pebble = new Pebble("bucket/obj2c", now);
             * pebble.setOwner("terry"); pebble.setMIME("txt/plain");
             * pebble.setChunkPrefix
             * (MD5HashUtil.hashCodeBytes(String.valueOf(now)));
             * PebbleDB.put(pebble); System.out.println(pebble); get =
             * PebbleDB.get(pebble.getPebbleID()); System.out.println(get);
             * //PebbleDB.remove(pebble.getPebbleID());
             * 
             * bucketRet = PebbleDB.scan("bucket", "obj2", 2);
             * printBucketRet(bucketRet);
             * 
             * 
             * bucketRet = PebbleDB.scan("bucket", "obj2", 2);
             * printBucketRet(bucketRet);
             * 
             * bucketRet = PebbleDB.scan("bucket", null, 10, "obj2", "2");
             * printBucketRet(bucketRet);
             * 
             * get = PebbleDB.get("bucket/obj1"); System.out.println(get); get =
             * PebbleDB.get(pebble.getPebbleID()); System.out.println(get);
             */
            Pebble p = PebbleDB.getPebble("pebble_0001/pebble_XXXX_00000001");
            System.out.println(p);
            System.out.println("--------------------------");
            Set<Chunk> chunks = null;

            chunks = ChunkDB.get(p.getChunkPrefix());
            for (Chunk chunk : chunks) {
                System.out.println("\t" + chunk);
            }

            Chunk chunk = chunks.iterator().next();
            RockAccessor.connectHDFS();
            RockReader rr = RockReaderPool.getInstance().get(
                    MD5HashUtil.hexStringFromBytes(chunk.getRockID()));

            if (rr == null) {
                System.out.println("some thing is wrong");
                return;
            }
            FileOutputStream fos = new FileOutputStream(new File("d:/r.txt"));
            rr.readChunk(chunk, fos);
            fos.flush();

            fos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
