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
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.rockstor.core.meta.Chunk;
import com.rockstor.core.meta.Pebble;
import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.HBaseClient;
import com.rockstor.util.MD5HashUtil;

public class GarbageChunkDB {
    public static Logger LOG = Logger.getLogger(GarbageChunkDB.class);

    public static final String TAB_NAME = HBaseClient.TABLE_GARBAGE_CHUNK;

    public static final byte[] COL_FAMILY_M = Bytes.toBytes("M");
    public static final byte[] QUALIFER_SIZE = Bytes.toBytes("size");

    public static void put(Pebble pebble) throws IOException {
        Put put = new Put(pebble.getChunkPrefix());
        put.add(COL_FAMILY_M, QUALIFER_SIZE, Bytes.toBytes(pebble.getSize()));
        HBaseClient.put(TAB_NAME, put);
        LOG.info("Add pebble to " + TAB_NAME + ", pebble = " + pebble
                + ", chunkPrefix = "
                + MD5HashUtil.hexStringFromBytes(pebble.getChunkPrefix()));
    }

    public static void put(List<Pebble> pebbles) throws IOException {
        List<Put> puts = new ArrayList<Put>();
        for (Pebble pebble : pebbles) {
            Put put = new Put(pebble.getChunkPrefix());
            put.add(COL_FAMILY_M, QUALIFER_SIZE,
                    Bytes.toBytes(pebble.getSize()));
            puts.add(put);
        }
        HBaseClient.put(TAB_NAME, puts);
        LOG.info("Add pebble to " + TAB_NAME + ", pebble num = "
                + pebbles.size());
    }

    public static void remove(byte[] chunkPrefix) throws IOException {
        Delete delete = new Delete(chunkPrefix);
        delete.deleteFamily(COL_FAMILY_M, System.currentTimeMillis());
        HBaseClient.delete(TAB_NAME, delete);
        LOG.info("Delete chunk from " + TAB_NAME + ", chunkPrefix = "
                + MD5HashUtil.hexStringFromBytes(chunkPrefix));
    }

    public static void remove(List<byte[]> chunkPrefixs) throws IOException {
        ArrayList<Delete> deletes = new ArrayList<Delete>();
        for (byte[] chunkPrefix : chunkPrefixs) {
            Delete delete = new Delete(chunkPrefix);
            delete.deleteFamily(COL_FAMILY_M, System.currentTimeMillis());
            deletes.add(delete);
        }
        HBaseClient.delete(TAB_NAME, deletes);
        LOG.info("Delete chunks(" + chunkPrefixs.size() + ") from " + TAB_NAME);
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
                chunk = new Chunk();

                Chunk.row2Chunk(result.getRow(), entry.getKey(),
                        entry.getValue());
                chunkSet.add(chunk);

            }
        } else { // get chunks while reading pebble

            short lastPartID = -1;
            for (Entry<byte[], byte[]> entry : fm.entrySet()) {
                chunk = Chunk.row2Chunk(result.getRow(), entry.getKey(),
                        entry.getValue());

                if (lastPartID != chunk.getPartID()) {
                    chunkSet.add(chunk);
                }

                lastPartID = chunk.getPartID();
            }
        }

        return chunkSet;
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
}
