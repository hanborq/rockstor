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
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.rockstor.core.meta.Chunk;
import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.HBaseClient;
import com.rockstor.util.MD5HashUtil;

/**
 * tools to maintain chunk data in hbase
 * 
 * @author terry
 * 
 */
public class ChunkDB {
    public static Logger LOG = Logger.getLogger(ChunkDB.class);

    public static final String TAB_NAME = HBaseClient.TABLE_CHUNK;
    public static final byte[] COL_FAMILY_M = Bytes.toBytes("M");

    /**
     * save chunk to hbase
     * 
     * @param chunk
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws NoSuchAlgorithmException
     */
    public static void put(Chunk chunk) throws IOException,
            NoSuchAlgorithmException {
        assert (chunk.valid());
        Put put = new Put(Chunk.chunk2RowKey(chunk));

        put.add(COL_FAMILY_M, Chunk.chunk2ColKey(chunk),
                Chunk.chunk2ColValue(chunk));

        HBaseClient.put(TAB_NAME, put);
        LOG.info("Add chunk to " + TAB_NAME + " chunk = " + chunk);
    }

    public static void put(List<Chunk> chunks) throws IOException {
        if (chunks == null || chunks.isEmpty()) {
            return;
        }

        List<Put> puts = new ArrayList<Put>();
        for (Chunk chunk : chunks) {
            assert (chunk.valid());
            Put put = new Put(Chunk.chunk2RowKey(chunk));
            put.add(COL_FAMILY_M, Chunk.chunk2ColKey(chunk),
                    Chunk.chunk2ColValue(chunk));
            puts.add(put);
        }

        HBaseClient.put(TAB_NAME, puts);
        LOG.info("Add chunks to " + TAB_NAME + " chunk num = " + chunks.size());
    }

    /**
     * update chunk
     * 
     * @param chunk
     *            only update
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws NoSuchAlgorithmException
     */
    public static void update(Chunk chunk) throws IOException,
            NoSuchAlgorithmException {
        put(chunk);
        LOG.info("Update chunk to " + TAB_NAME + " chunk = " + chunk);
    }

    /**
     * @param rockID
     * @param updates
     *            , chunks need to update the location after compressed
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void update(List<Chunk> updates) throws IOException {
        put(updates);
        LOG.info("Update chunks to " + TAB_NAME + " chunk num = "
                + updates.size());
    }

    public static Set<Chunk> parseResult(Result result, boolean readAll) {
        if (result == null) {
            return null;
        }

        NavigableMap<byte[], byte[]> fm = result.getFamilyMap(COL_FAMILY_M);
        if (fm == null || fm.isEmpty()) {
            return null;
        }

        Set<Chunk> chunkSet = new TreeSet<Chunk>();
        Chunk chunk = null;

        if (readAll) { // collect chunks while compacting
            for (Entry<byte[], byte[]> entry : fm.entrySet()) {
                chunk = Chunk.row2Chunk(result.getRow(), entry.getKey(),
                        entry.getValue());
                chunkSet.add(chunk);

            }
        } else { // get chunks while reading pebble

            int lastId = -1;
            int curId = -1;
            for (Entry<byte[], byte[]> entry : fm.entrySet()) {
                chunk = Chunk.row2Chunk(result.getRow(), entry.getKey(),
                        entry.getValue());
                curId = (((int) chunk.getPartID()) << 16) | chunk.getSeqID();
                if (lastId != curId) {
                    chunkSet.add(chunk);
                }

                lastId = curId;
            }
        }

        return chunkSet;
    }

    public static Set<Chunk> get(byte[] chunkPrefix) throws IOException {
        return get(chunkPrefix, false);
    }

    public static Set<Chunk> getAll(byte[] chunkPrefix) throws IOException {
        return get(chunkPrefix, true);
    }

    protected static Set<Chunk> get(byte[] chunkPrefix, boolean readAll)
            throws IOException {
        if (chunkPrefix == null) {
            LOG.error("Get chunk from " + TAB_NAME
                    + " error, chunkPrefix == null");
            return null;
        }

        Get g = new Get(chunkPrefix);
        g.addFamily(COL_FAMILY_M);

        Result result = HBaseClient.get(TAB_NAME, g);

        if (result == null || result.isEmpty()) {
            LOG.error("Get chunk from " + TAB_NAME
                    + " with empty return, chunkPrefix = "
                    + MD5HashUtil.hexStringFromBytes(chunkPrefix));
            return null;
        }

        try {
            return parseResult(result, readAll);
        } catch (Exception e) {
            LOG.error("Get chunk from " + TAB_NAME
                    + " with parse error, chunkPrefix = "
                    + MD5HashUtil.hexStringFromBytes(chunkPrefix));
            LOG.error(ExceptionLogger.getStack(e));
        }

        return null;
    }

    public static void remove(Chunk chunk) throws IOException,
            NoSuchAlgorithmException {
        assert (chunk.valid());
        Delete delete = new Delete(Chunk.chunk2RowKey(chunk));
        delete.deleteColumns(COL_FAMILY_M, Chunk.chunk2ColKey(chunk),
                System.currentTimeMillis());

        HBaseClient.delete(TAB_NAME, delete);
        LOG.info("Delete chunk from " + TAB_NAME + ", chunk = " + chunk);
    }

    // remove chunks whose rock file is not existed.
    public static void remove(List<Chunk> chunks) throws IOException,
            NoSuchAlgorithmException {
        ArrayList<Delete> deletes = new ArrayList<Delete>();
        long now = System.currentTimeMillis();
        for (Chunk chunk : chunks) {
            assert (chunk.valid());
            Delete delete = new Delete(Chunk.chunk2RowKey(chunk));
            delete.deleteColumn(COL_FAMILY_M, Chunk.chunk2ColKey(chunk), now);
            deletes.add(delete);
        }

        HBaseClient.delete(TAB_NAME, deletes);
        LOG.info("Delete chunk from " + TAB_NAME + ", chunk num = "
                + chunks.size());
    }

    /**
     * @param args
     */
    static void test_str() {
        NavigableMap<Long, Long> m = new TreeMap<Long, Long>();
        m.put(1L, 1L);
        m.put(5L, 5L);
        m.put(3L, 3L);
        System.out.println(m.lastEntry().getValue());
        System.out.println("");

        String[] testStrs = new String[] { "a", "a/", "a/b/", "abcd", "ab/",
                "abasdas/" };

        String prefix = "a";
        int prefixLen = prefix.length();
        String delimiter = "/";
        int delimiterLen = delimiter.length();
        int pos = 0;

        for (String str : testStrs) {
            pos = str.indexOf(delimiter, prefixLen);

            if (pos > 0) {
                System.out.println(str + ", TRUE, "
                        + str.substring(0, pos + delimiterLen));
            } else {
                System.out.println(str + ", FALSE");
            }
        }

        System.out.println("_____________________");
        delimiter = "b/";

        for (String str : testStrs) {
            pos = str.indexOf(delimiter, prefixLen);

            if (pos > 0) {
                System.out.println(str + ", TRUE, "
                        + str.substring(0, pos + delimiterLen));
            } else {
                System.out.println(str + ", FALSE");
            }
        }
        System.out.println("_____________________");
    }

}
