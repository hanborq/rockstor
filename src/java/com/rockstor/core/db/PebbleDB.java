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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.rockstor.core.meta.ACL;
import com.rockstor.core.meta.Pebble;
import com.rockstor.util.DBUtil;
import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.HBaseClient;

/**
 * tools to maintain pebble data in hbase
 * 
 * @author terry
 * 
 */
public class PebbleDB {
    public static Logger LOG = Logger.getLogger(PebbleDB.class);

    public static final String TAB_NAME = HBaseClient.TABLE_PEBBLE;

    public static final byte[] COL_FAMILY_M = Bytes.toBytes("M");
    public static final byte[] QUALIFER_CHUNK_PREFIX = Bytes
            .toBytes("chunkPrefix");
    public static final byte[] QUALIFER_CHUNK_NUMBER = Bytes
            .toBytes("chunkNum");
    public static final byte[] QUALIFER_SIZE = Bytes.toBytes("size");
    public static final byte[] QUALIFER_ETAG = Bytes.toBytes("etag");
    public static final byte[] QUALIFER_OWNER = Bytes.toBytes("owner");

    public static final byte[] QUALIFER_ACL = Bytes.toBytes("acl");
    public static final String QUALIFER_META_PREFIX_STR = "meta-";
    public static final byte[] QUALIFER_META_PREFIX_BYTES = Bytes
            .toBytes(QUALIFER_META_PREFIX_STR);
    public static final int QUALIFER_META_PREFIX_STR_LEN = QUALIFER_META_PREFIX_STR
            .length();
    public static final byte[] QUALIFER_MIME = Bytes.toBytes("mime");

    public static final byte[] ROW_KEY_SEP = new byte[] { 0 };
    private static final long DELETE_MARK_VERSION_VAL = Long.MAX_VALUE;
    private static final byte[] DELETE_MARK_VERSION = Bytes.toBytes(0L);
    private static final byte[] DELETE_MARK_SUBFIX = Bytes.add(ROW_KEY_SEP,
            DELETE_MARK_VERSION);

    public static final int ROW_KEY_SUFFIX_LEN = 9;

    public static void updateMeta(Pebble pebble, Map<String, String> metas)
            throws IOException, NoSuchAlgorithmException {
        if (pebble == null || metas == null || metas.isEmpty()) {
            return;
        }
        for (Entry<String, String> entry : metas.entrySet()) {
            pebble.addMeta(entry.getKey(), entry.getValue());
        }
        pebble.setEtag();
        Put put = new Put(getRowKey(pebble));
        put.add(COL_FAMILY_M, QUALIFER_ETAG, pebble.getEtag());

        // meta
        if (metas != null && !metas.isEmpty()) {
            for (Entry<String, String> meta : metas.entrySet()) {
                put.add(COL_FAMILY_M,
                        Bytes.toBytes(QUALIFER_META_PREFIX_STR + meta.getKey()),
                        Bytes.toBytes(meta.getValue()));
            }
        }

        HBaseClient.put(TAB_NAME, put);
        LOG.info("update pebble to " + TAB_NAME + " OK, pebble = " + pebble);
    }

    public static void deleteMeta(Pebble pebble, String[] metas)
            throws IOException {
        if (pebble == null) {
            return;
        }

        if (metas == null || metas.length == 0) {
            Map<String, String> oldMetas = pebble.getMeta();
            if (oldMetas == null || oldMetas.isEmpty()) {
                LOG.info("Delete from "
                        + TAB_NAME
                        + " CF<M> Qualify: rockstor-meta Failed, input is null, pebble = "
                        + pebble.getPebbleID() + ", version = "
                        + pebble.getVersion());
                return;
            }
            metas = oldMetas.keySet().toArray(new String[0]);
        }

        Delete delete = new Delete(getRowKey(pebble));

        for (String meta : metas) {
            delete.deleteColumns(COL_FAMILY_M,
                    Bytes.toBytes(QUALIFER_META_PREFIX_STR + meta));
        }

        HBaseClient.delete(TAB_NAME, delete);
        LOG.info("Delete from " + TAB_NAME
                + " CF<M> Qualify: rockstor-meta OK pebble = "
                + pebble.getPebbleID() + ", version = " + pebble.getVersion());
    }

    /**
     * save pebble to hbase
     * 
     * @param pebble
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws NoSuchAlgorithmException
     */
    public static void put(Pebble pebble) throws IOException,
            NoSuchAlgorithmException {
        pebble.setEtag();
        Put put = new Put(getRowKey(pebble));

        put.add(COL_FAMILY_M, QUALIFER_CHUNK_PREFIX, pebble.getChunkPrefix());
        put.add(COL_FAMILY_M, QUALIFER_CHUNK_NUMBER,
                Bytes.toBytes(pebble.getChunkNum()));
        put.add(COL_FAMILY_M, QUALIFER_SIZE, Bytes.toBytes(pebble.getSize()));
        put.add(COL_FAMILY_M, QUALIFER_ETAG, pebble.getEtag());
        put.add(COL_FAMILY_M, QUALIFER_OWNER, Bytes.toBytes(pebble.getOwner()));
        put.add(COL_FAMILY_M, QUALIFER_MIME, Bytes.toBytes(pebble.getMIME()));
        // meta
        HashMap<String, String> metas = pebble.getMeta();
        if (metas != null && !metas.isEmpty()) {
            for (Entry<String, String> meta : metas.entrySet()) {
                put.add(COL_FAMILY_M,
                        Bytes.toBytes(QUALIFER_META_PREFIX_STR + meta.getKey()),
                        Bytes.toBytes(meta.getKey()));
            }
        }
        // acl
        ACL acl = pebble.getAcl();
        byte[] aclBytes = null;
        if (acl != null) {
            aclBytes = ACL.toColumnValue(acl);
            if (aclBytes != null) {
                put.add(COL_FAMILY_M, QUALIFER_ACL, aclBytes);
            }
        }
        HBaseClient.put(TAB_NAME, put);
        LOG.info("Add pebble to " + TAB_NAME + " OK, pebble = " + pebble);
    }

    /**
     * update pebble
     * 
     * @param pebble
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws NoSuchAlgorithmException
     */
    public static void update(Pebble pebble) throws IOException,
            NoSuchAlgorithmException {
        pebble.setEtag();
        Put put = new Put(getRowKey(pebble));

        put.add(COL_FAMILY_M, QUALIFER_CHUNK_PREFIX, pebble.getChunkPrefix());
        put.add(COL_FAMILY_M, QUALIFER_CHUNK_NUMBER,
                Bytes.toBytes(pebble.getChunkNum()));
        put.add(COL_FAMILY_M, QUALIFER_SIZE, Bytes.toBytes(pebble.getSize()));
        put.add(COL_FAMILY_M, QUALIFER_ETAG, pebble.getEtag());
        put.add(COL_FAMILY_M, QUALIFER_OWNER, Bytes.toBytes(pebble.getOwner()));
        put.add(COL_FAMILY_M, QUALIFER_MIME, Bytes.toBytes(pebble.getMIME()));
        // meta
        HashMap<String, String> metas = pebble.getMeta();
        if (metas != null && !metas.isEmpty()) {
            for (Entry<String, String> meta : metas.entrySet()) {
                put.add(COL_FAMILY_M,
                        Bytes.toBytes(QUALIFER_META_PREFIX_STR + meta.getKey()),
                        Bytes.toBytes(meta.getKey()));
            }
        }

        // acl
        ACL acl = pebble.getAcl();
        byte[] aclBytes = null;
        if (acl != null) {
            aclBytes = ACL.toColumnValue(acl);
            if (aclBytes != null) {
                put.add(COL_FAMILY_M, QUALIFER_ACL, aclBytes);
            }
        }
        HBaseClient.put(TAB_NAME, put);
        LOG.info("Update pebble to " + TAB_NAME + " OK, pebble = " + pebble);
    }

    public static void remove(Pebble pebble) throws IOException,
            NoSuchAlgorithmException {
        remove(pebble.getPebbleID());
    }

    /**
     * delete pebble, now we just mark pebble as deleted, insert a pebble with
     * max version
     * 
     * @param pebbleID
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws NoSuchAlgorithmException
     */
    public static boolean remove(String pebbleID) throws IOException,
            NoSuchAlgorithmException {
        NavigableMap<Long, Pebble> m = getPebbleAllVersion(pebbleID);

        // pebble not exist
        if (m == null) {
            LOG.error("Get pebble from " + TAB_NAME
                    + " with null return: pebble = " + pebbleID);
            return false;
        }

        List<Pebble> pebbles = new ArrayList<Pebble>();
        pebbles.addAll(m.values());
        LOG.info("Get pebble from " + TAB_NAME + " with " + pebbles.size()
                + " instances, pebble = " + pebbleID);

        GarbageChunkDB.put(pebbles);
        LOG.info("Update GarbageChunkDB for pebble = " + pebbleID);

        removeMeta(pebbles);
        LOG.info("Delete pebble from " + TAB_NAME + "OK, pebble = " + pebbleID);
        return true;
    }

    /**
     * 
     * delete special version of pebble, if pebble not exist, do nothing, else
     * set COL_FAMILY_M:QUALIFER_DEL to true, and insert a record to rockdb
     * 
     * @param pebbleID
     * @param version
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws NoSuchAlgorithmException
     */
    public static boolean remove(String pebbleId, long version)
            throws IOException, NoSuchAlgorithmException {
        // check if pebble is exist
        if (version <= 0) {
            LOG.error("can not remove a special pebble with version less than zero, pebble = "
                    + pebbleId + ", version" + version);
            return false;
        }

        Pebble pebble = getPebbleWithVersion(pebbleId, version);
        if (pebble == null) {
            LOG.error("get pebble from " + TAB_NAME
                    + " with null return, pebble = " + pebbleId + ", version"
                    + version);
            return false;
        }

        GarbageChunkDB.put(pebble);
        LOG.info("Update GarbageChunkDB for pebble = " + pebbleId
                + ", version = " + version);

        removeMeta(pebble);
        LOG.info("Delete pebble from " + TAB_NAME + "OK, pebble = " + pebbleId
                + ", version = " + version);
        return true;
    }

    // remove pebble's meta
    public static void removeMeta(Pebble pebble) throws IOException {
        Delete delete = new Delete(getRowKey(pebble));
        delete.deleteFamily(COL_FAMILY_M, System.currentTimeMillis());
        HBaseClient.delete(TAB_NAME, delete);
        LOG.info("Delete from " + TAB_NAME + " CF<M> OK pebble = "
                + pebble.getPebbleID() + ", version = " + pebble.getVersion());
    }

    // remove pebble's meta
    public static void removeMeta(List<Pebble> pebbles) throws IOException {
        ArrayList<Delete> deletes = new ArrayList<Delete>();
        Delete delete = null;
        long now = System.currentTimeMillis();
        for (Pebble pebble : pebbles) {
            delete = new Delete(getRowKey(pebble));
            delete.deleteFamily(COL_FAMILY_M, now);
            deletes.add(delete);
        }
        HBaseClient.delete(TAB_NAME, deletes);
        LOG.info("Delete from " + TAB_NAME + " CF<M> OK pebble size = "
                + pebbles.size());
    }

    /**
     * get the latest version
     * 
     * @param pebbleID
     * @return null, if pebble is not exist, or is deleted; else pebble meta
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static Pebble get(String pebbleID) throws IOException {
        // get full map, as we should only fetch the latest version
        NavigableMap<Long, Pebble> nm = getPebble(pebbleID, true);

        if (nm == null || nm.isEmpty()) {
            LOG.error("Get pebble from " + TAB_NAME
                    + " with empty result, pebbleID = " + pebbleID);
            return null;
        }

        Pebble pebble = null;
        // System.out.println(nm.size());
        for (Entry<Long, Pebble> entry : nm.entrySet()) {
            // deleted row
            if (entry.getKey() == 0) {
                LOG.error("Cannot run to here, entry.getKey() == 0.");
                return null;
            }

            pebble = entry.getValue();

            LOG.info("Get pebble from " + TAB_NAME
                    + " OK, return the 1st pebble, pebble = " + pebbleID);
            return pebble;
        }

        LOG.error("Cannot run to here.");
        return null;
    }

    /**
     * get pebble instance from hbase scan result
     * 
     * @param result
     * @return
     */
    private static Pebble parseResult(Result result) {
        if (result == null || result.getRow() == null) {
            return null;
        }

        // check delete
        Pebble pebble = new Pebble();
        parseRowKey(result.getRow(), pebble);
        // System.out.println(pebble.getVersion() +" vs "+
        // DELETE_MARK_VERSION_VAL);
        if (pebble.getVersion() == DELETE_MARK_VERSION_VAL) {
            return pebble;
        }

        NavigableMap<byte[], byte[]> fm = result.getFamilyMap(COL_FAMILY_M);

        byte[] value = null;

        // LOG.info("column num: "+ fm.size());

        try {
            value = fm.remove(QUALIFER_ETAG);
            if (value != null) {
                pebble.setEtag(value);
            }

            value = fm.remove(QUALIFER_CHUNK_NUMBER);
            if (value != null) {
                pebble.setChunkNum(Bytes.toShort(value));
            }

            value = fm.remove(QUALIFER_SIZE);
            if (value != null) {
                pebble.setSize(Bytes.toLong(value));
            }

            value = fm.remove(QUALIFER_OWNER);
            if (value != null) {
                pebble.setOwner(Bytes.toString(value));
            }

            value = fm.remove(QUALIFER_CHUNK_PREFIX);
            if (value != null) {
                pebble.setChunkPrefix(value);
            }

            // get mime
            value = fm.remove(QUALIFER_MIME);
            if (value != null) {
                pebble.setMIME(Bytes.toString(value));
            }

            value = fm.remove(QUALIFER_ACL);
            if (value != null) {
                pebble.setAcl(ACL.fromColumnValue(value));
            }

            if (!fm.isEmpty()) {
                byte[] k_bytes = null;
                String k = null;
                String v = null;
                HashMap<String, String> metas = new HashMap<String, String>();
                for (Entry<byte[], byte[]> entry : fm.entrySet()) {
                    k_bytes = entry.getKey();
                    if (0 == Bytes.compareTo(k_bytes, 0,
                            QUALIFER_META_PREFIX_BYTES.length,
                            QUALIFER_META_PREFIX_BYTES, 0,
                            QUALIFER_META_PREFIX_BYTES.length)) {
                        k = Bytes.toString(entry.getKey()).substring(
                                QUALIFER_META_PREFIX_STR_LEN);
                        v = Bytes.toString(entry.getValue());
                        metas.put(k, v);
                    }
                }

                pebble.setMeta(metas);
            }
        } catch (Exception e) {
            LOG.error("parse Pebble Result Failed, Exception:"
                    + ExceptionLogger.getStack(e));
            return null;
        }

        return pebble;
    }

    /**
     * get special version of pebble
     * 
     * @param pebbleId
     * @param version
     * @return pebble meta, if exists; null, else
     */
    public static Pebble getPebbleWithVersion(String pebbleId, long version) {
        // can not fetch such version, only used locally
        if (version == DELETE_MARK_VERSION_VAL) {
            LOG.error("Get pebble from " + TAB_NAME + " error: version = "
                    + version);
            return null;
        }
        Get get = new Get(getRowKey(pebbleId, version));
        get.addFamily(COL_FAMILY_M);
        Result result;
        try {
            result = HBaseClient.get(TAB_NAME, get);
        } catch (Exception e) {
            LOG.error("Get pebble from " + TAB_NAME + " error: pebble = "
                    + pebbleId + ", version = " + version);
            LOG.error(ExceptionLogger.getStack(e));
            return null;
        }

        if (result == null) {
            LOG.error("Get pebble from " + TAB_NAME
                    + " with null return: pebble = " + pebbleId
                    + ", version = " + version);
            return null;
        }

        Pebble pebble = parseResult(result);

        LOG.info("Get pebble from " + TAB_NAME + " OK: pebble = " + pebbleId
                + ", version = " + version);
        return pebble;
    }

    /**
     * got latest version of pebble
     * 
     * @param pebbleId
     * @return null, if pebble is deleted
     */
    public static Pebble getPebble(String pebbleId) {
        return getPebbleLatestVersion(pebbleId);
    }

    public static Pebble getPebbleLatestVersion(String pebbleId) {
        // scan
        NavigableMap<Long, Pebble> m = getPebble(pebbleId, false);
        if (m == null) {
            return null;
        }

        Pebble pebble = m.firstEntry().getValue();

        return pebble;
    }

    /**
     * @param pebbleId
     * @return all versions of pebble, include version used as deleted mark
     */
    public static NavigableMap<Long, Pebble> getPebbleAllVersion(String pebbleId) {
        // scan
        NavigableMap<Long, Pebble> m = getPebble(pebbleId, true);
        if (m == null) {
            return null;
        }

        return m;
    }

    /**
     * @param pebbleId
     *            , pebbleID
     * @param version
     *            , version of pebble, 0, latest version; -1, all version; >1,
     *            special version
     * @return
     */
    private static NavigableMap<Long, Pebble> getPebble(String pebbleId,
            boolean allVersion) {
        if (pebbleId == null || pebbleId.isEmpty()) {
            LOG.error("Get pebble from " + TAB_NAME
                    + " error : pebbleId == null || pebbleId.isEmpty()");
            return null;
        }

        LOG.info("Get pebble from " + TAB_NAME + ": pebble = " + pebbleId
                + ", allVersion = " + allVersion);

        NavigableMap<Long, Pebble> ret = new TreeMap<Long, Pebble>();
        Pebble pebble = null;

        Scan scan = new Scan();
        scan.addFamily(COL_FAMILY_M);
        scan.setStartRow(getDeleteRowKey(pebbleId));
        scan.setStopRow(getRowKey(pebbleId, 0L));

        try {
            List<Result> results = DBUtil.scan(TAB_NAME, scan,
                    allVersion ? Integer.MAX_VALUE : 1);
            // LOG.info("got Result num: "+results.size()+" for pebble: "+pebbleId);
            for (Result result : results) {
                pebble = parseResult(result);
                // LOG.info(pebble);
                if (pebble != null) {
                    ret.put(Long.MAX_VALUE - pebble.getVersion(), pebble);
                }
            }
        } catch (Exception e) {
            LOG.error("Get pebble from " + TAB_NAME
                    + " catch exception, pebble = " + pebbleId);
            LOG.error(ExceptionLogger.getStack(e));
            return null;
        }

        if (ret.isEmpty()) {
            LOG.error("Get pebble from " + TAB_NAME
                    + " with empty result, pebble = " + pebbleId);
            return null;
        }

        LOG.info("Get pebble from " + TAB_NAME + " OK.");
        return ret;
    }

    // for client scan
    // return pair<pebbles, commom prefixs>
    public static Pair<ArrayList<Pebble>, ArrayList<String>> scan(
            String bucket, String marker, int maxKeys, String prefix,
            String delimiter) throws IOException {
        String realPrefix = bucket + "/";
        String realMarker = realPrefix;

        if (prefix != null && !prefix.isEmpty()) {
            realPrefix = bucket + "/" + prefix;
        }

        if (marker != null && !marker.isEmpty()) {
            realMarker = bucket + "/" + marker;
        }

        if (realMarker.compareTo(realPrefix) < 0) {
            realMarker = realPrefix;
        }

        byte[] startKey = DBUtil.makeStartKey(realMarker);
        byte[] stopKey = DBUtil.makeStopKey(realPrefix);

        if (maxKeys <= 0) {
            maxKeys = Integer.MAX_VALUE;
        }

        LOG.info("prefix len: " + realPrefix.length());
        ScanFilter sf = new ScanFilter(realPrefix.length(), delimiter, maxKeys,
                startKey, stopKey);

        Scan scan = new Scan();
        scan.addFamily(COL_FAMILY_M);
        scan.setStartRow(startKey);
        scan.setStopRow(stopKey);
        scan.setFilter(sf);

        List<Result> results = DBUtil.scan(TAB_NAME, scan);

        LOG.info("got result num: " + results.size());

        ArrayList<Pebble> pebbles = new ArrayList<Pebble>();
        ArrayList<String> commonPrefixs = new ArrayList<String>();

        String pebbleID = null;
        Pebble pebble = null;

        if (delimiter == null || delimiter.isEmpty()) {
            for (Result result : results) {
                pebble = parseResult(result);
                pebbles.add(pebble);
                pebbleID = pebble.getPebbleID();
                LOG.info("append pebble: " + pebbleID);
            }
        } else {
            int pos = 0;
            int prefixLen = realPrefix.length();
            int delimiterLen = delimiter.length();

            for (Result result : results) {
                pebble = parseResult(result);
                pebbleID = pebble.getPebbleID();

                pos = pebbleID.indexOf(delimiter, prefixLen);
                if (pos < 0) {
                    pebbles.add(pebble);
                    LOG.info("append pebble: " + pebbleID);
                } else {
                    // common prefix
                    commonPrefixs
                            .add(pebbleID.substring(0, pos + delimiterLen));
                    LOG.info("append commonPrefix: "
                            + pebbleID.substring(0, pos + delimiterLen));
                }
            }
        }

        return new Pair<ArrayList<Pebble>, ArrayList<String>>(pebbles,
                commonPrefixs);
    }

    public static Pair<ArrayList<Pebble>, ArrayList<String>> scan(
            String bucket, String marker, int maxKeys) throws IOException {
        return scan(bucket, marker, maxKeys, null, null);
    }

    public static String clean(String startKey, int maxKeys,
            HashSet<byte[]> liveRocks) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(COL_FAMILY_M);
        scan.setStartRow(DBUtil.makeStartKey(startKey));
        List<Result> results = DBUtil.scan(TAB_NAME, scan, maxKeys);
        return clean(results, liveRocks);
    }

    protected static String clean(List<Result> results,
            HashSet<byte[]> liveRocks) throws IOException {

        if (results == null || results.isEmpty())
            return null;

        String pebbleID = null;

        ArrayList<Delete> deletes = new ArrayList<Delete>();
        Delete del = null;

        Pebble pebble = null;

        for (Result result : results) {
            pebble = parseResult(result);

            // pebble is not deleted or rock file is existed, do nothing
            if (pebble.getVersion() == DELETE_MARK_VERSION_VAL)
                continue;
            // rock is not exist, we should delete pebble entry
            del = new Delete(getRowKey(pebble));
            del.deleteFamily(COL_FAMILY_M);
            deletes.add(del);
        }

        if (!deletes.isEmpty()) {
            HBaseClient.delete(TAB_NAME, deletes);
        }

        return pebbleID;
    }

    protected static void clean(String pebbleID,
            NavigableMap<Long, Pebble> pebbles, HashSet<byte[]> liveRocks)
            throws IOException {
        if (pebbles == null || pebbles.isEmpty())
            return;

        ArrayList<Delete> deletes = new ArrayList<Delete>();
        Delete del = null;

        Pebble pebble = null;

        for (Entry<Long, Pebble> entry : pebbles.entrySet()) {
            pebble = entry.getValue();

            // pebble is not deleted or rock file is existed, do nothing
            if (pebble.getVersion() == DELETE_MARK_VERSION_VAL)
                continue;
            // rock is not exist, we should delete pebble entry
            del = new Delete(getRowKey(pebble));
            del.deleteFamily(COL_FAMILY_M);
            deletes.add(del);
        }

        if (deletes.isEmpty())
            return;

        HBaseClient.delete(TAB_NAME, deletes);
    }

    /*
     * // clean pebble protected static void clean(String pebbleID, Result
     * result, HashSet<byte[]> liveRocks) throws InterruptedException,
     * ExecutionException { assert (liveRocks != null); NavigableMap<Long,
     * Pebble> pebbles = getPebbleAllVersion(pebbleID, result); clean(pebbleID,
     * pebbles, liveRocks); }
     */

    // clean single pebble
    public static void clean(String pebbleID, HashSet<byte[]> liveRocks)
            throws IOException {
        assert (liveRocks != null);
        NavigableMap<Long, Pebble> pebbles = getPebbleAllVersion(pebbleID);
        clean(pebbleID, pebbles, liveRocks);
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

    public static byte[] getRowKey(String pebbleId, long version) {
        return Bytes
                .add(Bytes.toBytes(pebbleId),
                        Bytes.add(ROW_KEY_SEP,
                                Bytes.toBytes(Long.MAX_VALUE - version)));
    }

    public static byte[] getDeleteRowKey(String pebbleId) {
        return Bytes.add(Bytes.toBytes(pebbleId), DELETE_MARK_SUBFIX);
    }

    public static byte[] getRowKey(Pebble pebble) {
        return Bytes.add(
                Bytes.toBytes(pebble.getPebbleID()),
                Bytes.add(ROW_KEY_SEP,
                        Bytes.toBytes(Long.MAX_VALUE - pebble.getVersion())));
    }

    public static byte[] getDeleteRowKey(Pebble pebble) {
        return Bytes.add(Bytes.toBytes(pebble.getPebbleID()),
                DELETE_MARK_SUBFIX);
    }

    public static void parseRowKey(byte[] rowKey, Pebble p) {
        assert (rowKey.length > ROW_KEY_SUFFIX_LEN);
        int pos = rowKey.length - ROW_KEY_SUFFIX_LEN;
        p.setPebbleID(Bytes.toString(rowKey, 0, pos));
        // System.out.println(rowKey.length+" "+Bytes.toLong(rowKey,pos+1));
        p.setVersion(Long.MAX_VALUE - Bytes.toLong(rowKey, pos + 1));
    }

    public static Pair<String, Long> parseRowKey(byte[] rowKey) {
        assert (rowKey.length > ROW_KEY_SUFFIX_LEN);
        int pos = rowKey.length - ROW_KEY_SUFFIX_LEN;

        Pair<String, Long> p = new Pair<String, Long>();
        p.setFirst(Bytes.toString(rowKey, 0, pos));
        p.setSecond(Long.MAX_VALUE - Bytes.toLong(rowKey, pos + 1));
        return p;
    }

    public static void parseRowKey(byte[] buffer, int offset, int length,
            Pebble p) {
        assert (length > ROW_KEY_SUFFIX_LEN);
        int pos = length - ROW_KEY_SUFFIX_LEN;

        p.setPebbleID(Bytes.toString(buffer, offset, pos));
        p.setVersion(Long.MAX_VALUE - Bytes.toLong(buffer, offset + pos + 1));
    }

    public static Pair<String, Long> parseRowKey(byte[] buffer, int offset,
            int length) {
        assert (length > ROW_KEY_SUFFIX_LEN);
        int pos = length - ROW_KEY_SUFFIX_LEN;

        Pair<String, Long> p = new Pair<String, Long>();
        p.setFirst(Bytes.toString(buffer, offset, pos));
        p.setSecond(Long.MAX_VALUE - Bytes.toLong(buffer, offset + pos + 1));
        return p;
    }

    public static void main1(String[] args) {
        TreeMap<byte[], Pebble> m = new TreeMap<byte[], Pebble>(
                new Comparator<byte[]>() {
                    @Override
                    public int compare(byte[] left, byte[] right) {
                        return Bytes.compareTo(left, right);
                    }
                });

        Vector<Pebble> v = new Vector<Pebble>();
        long now = System.currentTimeMillis();
        Pebble pebble = null;
        for (int i = 0; i < 2; i++) {
            pebble = new Pebble("bucket/object1");
            pebble.setVersion(now + i * 1000);
            v.add(pebble);
        }

        for (int i = 0; i < 2; i++) {
            pebble = new Pebble("bucket/object0");
            pebble.setVersion(now + i * 1000);
            v.add(pebble);
        }

        for (int i = 0; i < 2; i++) {
            pebble = new Pebble("bucket/object");
            pebble.setVersion(now + i * 1000);
            v.add(pebble);
        }

        pebble = new Pebble("bucket/object1");
        pebble.setVersion(Long.MAX_VALUE);
        v.add(pebble);

        pebble = new Pebble("bucket/object");
        pebble.setVersion(Long.MAX_VALUE);
        v.add(pebble);

        pebble = new Pebble("bucket/object0");
        pebble.setVersion(Long.MAX_VALUE);
        v.add(pebble);

        Random r = new Random();
        int idx = 0;
        while (v.size() > 0) {
            idx = r.nextInt(v.size());
            pebble = v.remove(idx);
            m.put(getRowKey(pebble), pebble);
        }

        for (Entry<byte[], Pebble> item : m.entrySet()) {
            pebble = item.getValue();
            System.out.println(pebble.getPebbleID() + ", v "
                    + pebble.getVersion());
        }
    }
}
