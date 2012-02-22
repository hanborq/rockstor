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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.rockstor.core.meta.Chunk;
import com.rockstor.core.meta.Rock;
import com.rockstor.util.DBUtil;
import com.rockstor.util.HBaseClient;
import com.rockstor.util.MD5HashUtil;

/**
 * @author terry
 * 
 */

public class RockDB {
    public static Logger LOG = Logger.getLogger(RockDB.class);

    public static final String TAB_NAME = HBaseClient.TABLE_ROCK;
    public static final byte[] COL_FAMILY_M = Bytes.toBytes("M");
    public static final byte[] QUALIFER_CTIME = Bytes.toBytes("ctime"); // create
                                                                        // time
                                                                        // public
                                                                        // static
                                                                        // final
                                                                        // byte[]
                                                                        // QUALIFER_OTIME
                                                                        // =
                                                                        // Bytes.toBytes("otime");
                                                                        // //
                                                                        // close
    // time
    public static final byte[] QUALIFER_VERSION = Bytes.toBytes("version");// rock
                                                                           // version
    public static final byte[] QUALIFER_WRITER = Bytes.toBytes("writer");

    public static final byte[] QUALIFER_RETIRE = Bytes.toBytes("retire");

    // public static final byte[] QUALIFER_HEIR = Bytes.toBytes("heir");

    public static final byte[] QUALIFER_GARBAGE_BYTES = Bytes.toBytes("gb");
    public static final byte[] QUALIFER_DELETED_BYTES = Bytes.toBytes("db");

    public static final byte[] COL_FAMILY_G = Bytes.toBytes("G");

    /**
     * register rock when create rock
     * 
     * @param rock
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void create(Rock rock) throws IOException {
        Put put = new Put(rock.getRockMagic());
        put.add(COL_FAMILY_M, QUALIFER_CTIME, Bytes.toBytes(rock.getCtime()));
        put.add(COL_FAMILY_M, QUALIFER_VERSION,
                Bytes.toBytes(rock.getRockVersion()));
        String writer = rock.getWriter();
        if (writer != null && !writer.isEmpty()) {
            put.add(COL_FAMILY_M, QUALIFER_WRITER,
                    Bytes.toBytes(rock.getWriter()));
        }
        put.add(COL_FAMILY_M, QUALIFER_GARBAGE_BYTES,
                Bytes.toBytes(rock.getGarbageBytes()));
        put.add(COL_FAMILY_M, QUALIFER_DELETED_BYTES,
                Bytes.toBytes(rock.getDeleteBytes()));
        HBaseClient.put(TAB_NAME, put);
        LOG.info("Add Rock to " + TAB_NAME + ": " + rock.toString());
        LOG.info("rock version: " + rock.getRockVersion());
    }

    /**
     * 
     * @param rock
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void close(Rock rock) throws IOException {
        /*
         * rock.setOtime(System.currentTimeMillis()); Put put = new
         * Put(rock.getRockMagic()); put.add(COL_FAMILY_M,QUALIFER_OTIME,
         * Bytes.toBytes(rock.getOtime())); HBaseClient.put(TAB_NAME, put);
         */
        Delete delete = new Delete(rock.getRockMagic());
        delete.deleteColumns(COL_FAMILY_M, QUALIFER_WRITER,
                System.currentTimeMillis());
        try {
            HBaseClient.delete(TAB_NAME, delete);
        } catch (IllegalArgumentException e) {

        }
        LOG.info("Close Rock in " + TAB_NAME + ": " + rock.toString());
    }

    public static void retire(byte[][] rocks) throws IOException {
        List<Put> puts = new ArrayList<Put>();
        for (byte[] rock : rocks) {
            Put put = new Put(rock);
            put.add(COL_FAMILY_M, QUALIFER_RETIRE, Bytes.toBytes(true));
            puts.add(put);
        }

        HBaseClient.put(TAB_NAME, puts);
        for (byte[] rock : rocks) {
            LOG.info("Retire Rock in " + TAB_NAME + ": "
                    + MD5HashUtil.hexStringFromBytes(rock));
        }
    }

    public static void retire(Collection<byte[]> rocks) throws IOException {
        List<Put> puts = new ArrayList<Put>();
        for (byte[] rock : rocks) {
            Put put = new Put(rock);
            put.add(COL_FAMILY_M, QUALIFER_RETIRE, Bytes.toBytes(true));
            puts.add(put);
        }

        HBaseClient.put(TAB_NAME, puts);
        for (byte[] rock : rocks) {
            LOG.info("Retire Rock in " + TAB_NAME + ": "
                    + MD5HashUtil.hexStringFromBytes(rock));
        }
    }

    /**
     * @param rock
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void retire(Rock rock) throws IOException {
        Put put = new Put(rock.getRockMagic());
        put.add(COL_FAMILY_M, QUALIFER_RETIRE, Bytes.toBytes(true));
        HBaseClient.put(TAB_NAME, put);
        LOG.info("Retire Rock in " + TAB_NAME + ": " + rock.getRockID());
    }

    public static void remove(List<Rock> rocks) throws IOException {
        ArrayList<Delete> deletes = new ArrayList<Delete>();
        Delete delete = null;
        long now = System.currentTimeMillis();
        for (Rock rock : rocks) {
            delete = new Delete(rock.getRockMagic());
            delete.deleteFamily(COL_FAMILY_M, now);
            delete.deleteFamily(COL_FAMILY_G, now);
            deletes.add(delete);
        }
        HBaseClient.delete(TAB_NAME, deletes);
        for (Rock rock : rocks) {
            LOG.info("Delete Rock in " + TAB_NAME + ": " + rock.getRockID());
        }
    }

    public static void remove(byte[] rockId) throws IOException {
        Delete delete = new Delete(rockId);
        long now = System.currentTimeMillis();
        delete.deleteFamily(COL_FAMILY_M, now);
        delete.deleteFamily(COL_FAMILY_G, now);
        HBaseClient.delete(TAB_NAME, delete);
        LOG.info("Delete Rock in " + TAB_NAME + ": "
                + MD5HashUtil.hexStringFromBytes(rockId));
    }

    public static void remove(Rock rock) throws IOException {
        remove(rock.getRockMagic());
    }

    public static Map<String, Rock> getRocks(byte[] startRow, int num,
            Filter filter) throws IOException {
        Rock rock = null;
        Scan scan = new Scan();
        scan.addFamily(COL_FAMILY_M);

        if (filter != null) {
            scan.setFilter(filter);
        }

        List<Result> results = DBUtil.scan(TAB_NAME, scan, num, startRow);

        HashMap<String, Rock> rockMap = new HashMap<String, Rock>();
        for (Result r : results) {
            rock = getMeta(r);
            if (rock != null) {
                rockMap.put(rock.getRockID(), rock);
            }
        }

        return rockMap;
    }

    public static Map<String, Rock> getRocks() throws IOException {
        return getRocks(null, 0, null);
    }

    public static Map<String, Rock> getRocks(Filter filter) throws IOException {
        return getRocks(null, 0, filter);
    }

    public static Map<String, Rock> getRocks(byte[] startRow, int num)
            throws IOException {
        return getRocks(startRow, num, null);
    }

    /**
     * get the latest version
     * 
     * @param pebbleID
     * @return null, if pebble is not exist, or is deleted; else pebble meta
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static Rock getMeta(byte[] rockID) throws IOException,
            IllegalArgumentException {
        Get g = new Get(rockID);
        g.addFamily(COL_FAMILY_M);
        Result result = HBaseClient.get(TAB_NAME, g);
        LOG.info("Query Rock: " + MD5HashUtil.hexStringFromBytes(rockID));
        return getMeta(result);
    }

    private static Rock getMeta(Result result) {
        if (result == null || result.isEmpty()) {
            return null;
        }

        NavigableMap<byte[], byte[]> family = result.getFamilyMap(COL_FAMILY_M);

        if (family == null || family.isEmpty()) {
            return null;
        }

        Rock rock = new Rock(result.getRow());

        rock.setRockVersion(Bytes.toInt(family.get(QUALIFER_VERSION)));
        rock.setCtime(Bytes.toLong(family.get(QUALIFER_CTIME)));

        byte[] value = null;// family.get(QUALIFER_OTIME);
        // rock.setOtime(value==null?0:Bytes.toLong(value));

        value = family.get(QUALIFER_WRITER);
        rock.setWriter(value == null ? "" : Bytes.toString(value));

        value = family.get(QUALIFER_RETIRE);
        rock.setRetire(value == null ? false : Bytes.toBoolean(value));

        value = family.get(QUALIFER_GARBAGE_BYTES);
        rock.setGarbageBytes(value == null ? 0 : Bytes.toLong(value));

        value = family.get(QUALIFER_DELETED_BYTES);
        rock.setDeleteBytes(value == null ? 0 : Bytes.toLong(value));

        LOG.info("GET Rock: " + rock.toString() + ", rock version: "
                + Bytes.toInt(family.get(QUALIFER_VERSION)));
        return rock;
    }

    // only save write failed garbage chunk
    public static void putGarbage(Rock rock, long offset, long size)
            throws IOException {
        Put put = new Put(rock.getRockMagic());
        rock.addGarbageBytes(size);
        put.add(COL_FAMILY_G, Bytes.toBytes(offset), Bytes.toBytes(size));
        put.add(COL_FAMILY_M, QUALIFER_GARBAGE_BYTES,
                Bytes.toBytes(rock.getGarbageBytes()));
        HBaseClient.put(TAB_NAME, put);
        LOG.info("Save garbage info to " + TAB_NAME + ", rock = "
                + rock.getRockID() + ", offset = " + offset + ", size = "
                + size + ", garbageBytes = " + rock.getGarbageBytes());
    }

    public static void putGarbage(byte[] rockId, long offset, long size)
            throws IOException {
        Put put = new Put(rockId);
        put.add(COL_FAMILY_G, Bytes.toBytes(offset), Bytes.toBytes(size));
        HBaseClient.put(TAB_NAME, put);
        LOG.info("Save garbage info to " + TAB_NAME + ", rock = "
                + MD5HashUtil.hexStringFromBytes(rockId) + ", offset = "
                + offset + ", size = " + size);
    }

    public static void putGarbage(List<Chunk> dstChunks) throws IOException {
        if (dstChunks == null || dstChunks.isEmpty()) {
            return;
        }
        Put put = null;
        List<Put> puts = new ArrayList<Put>();
        long curSize = 0;
        long totalSize = 0;
        for (Chunk chunk : dstChunks) {
            put = new Put(chunk.getRockID());
            curSize = chunk.getSize() + Chunk.HEADER_LEN;
            totalSize += curSize;
            put.add(COL_FAMILY_G, Bytes.toBytes(chunk.getOffset()),
                    Bytes.toBytes(curSize));
            puts.add(put);
        }

        HBaseClient.put(TAB_NAME, puts);
        LOG.info("Save garbage info to " + TAB_NAME + ", chunksNum="
                + dstChunks.size() + ", size = " + totalSize);
    }

    public static Map<Long, Long> getGarbages(byte[] rockId) throws IOException {
        Map<Long, Long> m = new TreeMap<Long, Long>();

        Get g = new Get(rockId);
        g.addFamily(COL_FAMILY_G);
        Result result = HBaseClient.get(TAB_NAME, g);

        if (result == null || result.isEmpty() || result.getRow() == null) {
            return m;
        }

        NavigableMap<byte[], byte[]> family = result.getFamilyMap(COL_FAMILY_G);

        if (family == null || family.isEmpty()) {
            return m;
        }

        for (Entry<byte[], byte[]> entry : family.entrySet()) {
            m.put(Bytes.toLong(entry.getKey()), Bytes.toLong(entry.getValue()));
        }

        return m;
    }

    public static void updateDeleteSize(byte[] rockId, long size)
            throws IOException {
        Put put = new Put(rockId);
        put.add(COL_FAMILY_M, QUALIFER_DELETED_BYTES, Bytes.toBytes(size));
        HBaseClient.put(TAB_NAME, put);
        LOG.info("Update DeleteSize in " + TAB_NAME + ", rock = "
                + MD5HashUtil.hexStringFromBytes(rockId) + ", size = " + size);
    }

    /*
     * public static List<Pair<Long, Long>> getDeletedPebble(byte[] rockID)
     * throws IOException { Get g = new Get(rockID); g.addFamily(COL_FAMILY_G);
     * Result result = HBaseClient.get(TAB_NAME, g);
     * 
     * return getDeletedPebble(result); }
     * 
     * private static List<Pair<Long, Long>> getDeletedPebble(Result result) {
     * if (result == null || result.isEmpty()) { return null; }
     * 
     * NavigableMap<byte[], byte[]> family = result.getFamilyMap(COL_FAMILY_G);
     * 
     * if (family == null || family.isEmpty()) { return null; }
     * 
     * List<Pair<Long, Long>> lp = new ArrayList<Pair<Long, Long>>();
     * 
     * for (Entry<byte[], byte[]> entry : family.entrySet()) { byte[] value =
     * entry.getValue(); lp.add(new Pair<Long, Long>(Bytes.toLong(value, 0,
     * Bytes.SIZEOF_LONG), Bytes.toLong(value, Bytes.SIZEOF_LONG,
     * Bytes.SIZEOF_LONG))); }
     * 
     * return lp; }
     */

    /**
     * @param args
     */
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.137.15");

        conf.set("hbase.zookeeper.property.clientPort", "2181");
        System.out.println("Begin to init HBaseClient ok!");
        HBaseClient.init(conf, 10);
        try {
            Map<String, Rock> rocks = RockDB.getRocks();

            for (Entry<String, Rock> entry : rocks.entrySet()) {
                LOG.error(entry.getValue());
                // rockIds.add(entry.getValue().getRockMagic());
                // }
                //
                // // RockDB.retire(rockIds);
                // rocks = RockDB.getRocks();
                // for(Entry<String,Rock> entry:rocks.entrySet()){
                // LOG.error(entry.getValue());
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
