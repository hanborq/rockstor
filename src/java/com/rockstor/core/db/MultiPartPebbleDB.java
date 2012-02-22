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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.rockstor.core.meta.ACL;
import com.rockstor.core.meta.MultiPartPebble;
import com.rockstor.core.meta.Pebble;
import com.rockstor.util.DBUtil;
import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.HBaseClient;
import com.rockstor.util.UploadIdGen;

/**
 * tools to maintain pebble data in hbase
 * 
 * @author terry
 * 
 */
public class MultiPartPebbleDB {
    public static Logger LOG = Logger.getLogger(MultiPartPebbleDB.class);

    public static final String TAB_NAME = HBaseClient.TABLE_MULTI_PART_PEBBLE;

    public static final byte[] COL_FAMILY_M = Bytes.toBytes("M");
    public static final byte[] QUALIFER_CHUNK_PREFIX = Bytes
            .toBytes("chunkPrefix");
    public static final byte[] QUALIFER_CHUNK_NUMBER = Bytes
            .toBytes("chunkNum");
    public static final byte[] QUALIFER_SIZE = Bytes.toBytes("size");
    public static final byte[] QUALIFER_ETAG = Bytes.toBytes("etag");
    public static final byte[] QUALIFER_OWNER = Bytes.toBytes("owner");

    public static final byte[] QUALIFER_ACL = Bytes.toBytes("acl");
    public static final byte[] QUALIFER_META = Bytes.toBytes("meta");
    public static final byte[] QUALIFER_MIME = Bytes.toBytes("mime");

    public static final byte[] ROW_KEY_SEP = new byte[] { 0 };
    private static final long DELETE_MARK_VERSION_VAL = Long.MAX_VALUE;
    private static final byte[] DELETE_MARK_VERSION = Bytes.toBytes(0L);
    private static final byte[] DELETE_MARK_SUBFIX = Bytes.add(ROW_KEY_SEP,
            DELETE_MARK_VERSION);

    public static final int ROW_KEY_SUBFIX_LEN = 9;

    public static final String PART_PREFIX = "part-";

    public static final int PART_PREFIX_LEN = PART_PREFIX.length();

    /**
     * save pebble to hbase
     * 
     * @param pebble
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws NoSuchAlgorithmException
     */
    public static void create(Pebble pebble) throws IOException,
            NoSuchAlgorithmException {
        pebble.setEtag();
        Put put = new Put(getRowKey(pebble));

        put.add(COL_FAMILY_M, QUALIFER_CHUNK_PREFIX, pebble.getChunkPrefix());
        // put.add(COL_FAMILY_M, QUALIFER_CHUNK_NUMBER,
        // Bytes.toBytes(pebble.getChunkNum()));
        // put.add(COL_FAMILY_M, QUALIFER_SIZE,
        // Bytes.toBytes(pebble.getSize()));
        // put.add(COL_FAMILY_M, QUALIFER_ETAG, pebble.getEtag());
        put.add(COL_FAMILY_M, QUALIFER_OWNER, Bytes.toBytes(pebble.getOwner()));
        put.add(COL_FAMILY_M, QUALIFER_MIME, Bytes.toBytes(pebble.getMIME()));
        // meta
        byte[] metaBytes = Pebble.Meta2ColumnValue(pebble);
        if (metaBytes != null) {
            put.add(COL_FAMILY_M, QUALIFER_ACL, metaBytes);
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
        LOG.info("Add pebble to " + TAB_NAME + ", pebble = " + pebble);
    }

    /**
     * update pebble
     * 
     * @param pebble
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws NoSuchAlgorithmException
     */
    public static Pebble complete(Pebble pebble)
            throws NoSuchAlgorithmException, IOException {
        MultiPartPebble multiPartPebble = (MultiPartPebble) pebble;
        Pebble savePebble = multiPartPebble.toPebble();
        PebbleDB.put(savePebble);

        Delete delete = new Delete(getRowKey(pebble));
        delete.deleteFamily(COL_FAMILY_M, System.currentTimeMillis());
        HBaseClient.delete(TAB_NAME, delete);

        LOG.info("Delete pebble from " + TAB_NAME + "(complete), pebble = "
                + pebble);
        return savePebble;
    }

    public static void abort(Pebble pebble) throws IOException {
        GarbageChunkDB.put(pebble);
        Delete delete = new Delete(getRowKey(pebble));
        delete.deleteFamily(COL_FAMILY_M, System.currentTimeMillis());
        HBaseClient.delete(TAB_NAME, delete);
        LOG.info("Delete pebble from " + TAB_NAME + "(abort), pebble = "
                + pebble);
    }

    public static void addPart(Pebble pebble, MultiPartPebble.PartInfo part)
            throws IOException {
        Put put = new Put(getRowKey(pebble));
        put.add(COL_FAMILY_M, MultiPartPebble.PartInfo.toColKey(part),
                MultiPartPebble.PartInfo.toColValue(part));
        HBaseClient.put(TAB_NAME, put);
        LOG.info("Add pebble part to " + TAB_NAME + ", pebble = "
                + pebble.getPebbleID() + ", part = " + part);
    }

    /**
     * get pebble instance from hbase scan result
     * 
     * @param result
     * @return
     */
    private static MultiPartPebble parseResult(Result result) {
        if (result == null || result.getRow() == null) {
            return null;
        }

        // check delete
        MultiPartPebble pebble = new MultiPartPebble();

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
            /*
             * value = fm.get(QUALIFER_ETAG); if(value!=null){
             * pebble.setEtag(value); }
             * 
             * value = fm.get(QUALIFER_CHUNK_NUMBER); if(value!=null){
             * pebble.setChunkNum(Bytes.toShort(value)); }
             * 
             * value = fm.get(QUALIFER_SIZE); if(value!=null){
             * pebble.setSize(Bytes.toLong(value)); }
             */

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

            value = fm.remove(QUALIFER_META);
            if (value != null) {
                pebble.setMeta(Pebble.MetaFromColumnValue(value));
            }

            value = fm.remove(QUALIFER_ACL);
            if (value != null) {
                pebble.setAcl(ACL.fromColumnValue(value));
            }

            String key = null;
            for (Entry<byte[], byte[]> entry : fm.entrySet()) {
                key = Bytes.toString(entry.getKey());
                if (key.startsWith(PART_PREFIX)) {
                    pebble.addPart(MultiPartPebble.PartInfo.toPartInfo(
                            entry.getKey(), entry.getValue()));
                }
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
            LOG.error("Get pebble from " + TAB_NAME + " error, version = "
                    + version);
            return null;
        }
        Get get = new Get(getRowKey(pebbleId, version));
        get.addFamily(COL_FAMILY_M);
        Result result;
        try {
            result = HBaseClient.get(TAB_NAME, get);
        } catch (Exception e) {
            LOG.error("Get pebble from " + TAB_NAME
                    + " catch exception, pebble = " + pebbleId + ", version = "
                    + version);
            LOG.error(ExceptionLogger.getStack(e));
            return null;
        }

        if (result == null) {
            LOG.error("Get pebble from " + TAB_NAME
                    + " with null return, pebble = " + pebbleId
                    + ", version = " + version);
            return null;
        }

        Pebble pebble = parseResult(result);
        LOG.info("Get pebble from " + TAB_NAME + " OK, pebble = " + pebbleId
                + ", version = " + version);

        return pebble;
    }

    static class ScanFilter extends FilterBase {
        public static Logger LOG = Logger.getLogger(ScanFilter.class);

        private String delimiter = "";

        private int delimiterLen = 0;
        private int prefixLen = 0;
        private int maxKeys = Integer.MAX_VALUE;

        private byte[] skipEndKey = null;
        private Pair<String, Long> idVersion = null;

        String rowKey = null;
        long version = 0;
        int pos = 0;

        private boolean dropRow = false;
        private int colNum = 0;

        @Override
        public String toString() {
            return "[ScanFilter: " + ", prefixLen=" + this.prefixLen
                    + ", delimiter=" + this.delimiter + ", delimiterLen="
                    + this.delimiterLen + ", maxKeys=" + this.maxKeys + "]";
        }

        public ScanFilter() {
            super();
        }

        public ScanFilter(int prefixLen, String delimiter, int maxKeys,
                byte[] startKey, byte[] stopKey) {
            super();
            this.prefixLen = prefixLen;

            if (delimiter == null) {
                this.delimiter = "";
            } else {
                this.delimiter = delimiter;
                this.delimiterLen = this.delimiter.length();
            }

            this.skipEndKey = null;
            this.maxKeys = maxKeys;
            this.dropRow = false;

            LOG.info(this.toString() + " initialized!");
        }

        @Override
        public boolean hasFilterRow() {
            return false;
        }

        @Override
        public boolean filterRow() {
            if (dropRow || rowKey == null || colNum == 0) {
                return true;
            }

            LOG.debug("proc row: " + rowKey + ", v " + version + ", colNum: "
                    + colNum);

            // check delimiter
            // not filter delimiter
            if (delimiter.isEmpty()) {
                --maxKeys;
                // only fetch one version per pebble
                // skipEndKey = DBUtil.makeStartKey(rowKey);
            } else {
                pos = rowKey.indexOf(delimiter, prefixLen);
                if (pos < 0) {
                    --maxKeys;
                    // skipEndKey = DBUtil.makeStartKey(rowKey);
                } else {
                    // common prefix, only reserver one
                    skipEndKey = DBUtil.makeStopKey(rowKey.substring(0, pos
                            + delimiterLen));

                    LOG.debug("New Skip Key: "
                            + rowKey.substring(0, pos + delimiterLen));
                }
            }

            LOG.debug("append row: " + rowKey + ", left num: " + maxKeys);

            return false;
        }

        @Override
        public void reset() {
            rowKey = null;
            dropRow = false;
            colNum = 0;
        }

        @Override
        public ReturnCode filterKeyValue(KeyValue kv) {
            ++colNum;

            return ReturnCode.INCLUDE;
        }

        @Override
        public boolean filterRowKey(byte[] buffer, int offset, int length) {
            idVersion = parseRowKey(buffer, offset, length);
            rowKey = idVersion.getFirst();
            version = idVersion.getSecond();

            LOG.debug("Row: "
                    + rowKey
                    + ", v "
                    + version
                    + ", "
                    + Bytes.toStringBinary(buffer, offset, length
                            - MultiPartPebbleDB.ROW_KEY_SUBFIX_LEN));

            // skip common prefix
            if (skipEndKey != null
                    && Bytes.compareTo(buffer, offset, length
                            - MultiPartPebbleDB.ROW_KEY_SUBFIX_LEN, skipEndKey,
                            0, skipEndKey.length) <= 0) {
                LOG.debug("ignore row: " + rowKey + ", wait for next check row");
                return true;
            }

            return false;
        }

        /**
         * If this returns true, the scan will terminate.
         * 
         * @return true to end scan, false to continue.
         */
        @Override
        public boolean filterAllRemaining() {
            return maxKeys < 0;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.prefixLen = in.readInt();
            this.delimiter = in.readUTF();
            this.maxKeys = in.readInt();

            this.delimiterLen = this.delimiter.length();
            this.skipEndKey = null;
            this.dropRow = false;

            LOG.debug(this.toString() + " decoded ok!");
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.prefixLen);
            out.writeUTF(this.delimiter);
            out.writeInt(maxKeys);
        }
    }

    // for client scan
    // return pair<pebbles, commom prefixs>
    public static Pair<ArrayList<MultiPartPebble>, ArrayList<String>> scan(
            String bucket, String keyMarker, String uploadIdMarker,
            int maxUploads, String prefix, String delimiter)
            throws IOException, NoSuchAlgorithmException {
        String realPrefix = bucket + "/";
        String realMarker = realPrefix;

        if (prefix != null && !prefix.isEmpty()) {
            realPrefix = bucket + "/" + prefix;
        }

        if (keyMarker != null && !keyMarker.isEmpty()) {
            realMarker = bucket + "/" + keyMarker;
        }

        if (realMarker.compareTo(realPrefix) < 0) {
            realMarker = realPrefix;
            uploadIdMarker = null;
        }

        byte[] startKey = null;

        // modify start key
        if (uploadIdMarker != null && !uploadIdMarker.isEmpty()) {
            startKey = DBUtil.makeStartKey(getRowKey(realMarker,
                    UploadIdGen.uploadID2Ver(realMarker, uploadIdMarker)));
        } else {
            startKey = DBUtil.makeStartKey(realMarker);
        }

        byte[] stopKey = DBUtil.makeStopKey(realPrefix);

        if (maxUploads <= 0) {
            maxUploads = Integer.MAX_VALUE;
        }

        LOG.info("prefix len: " + realPrefix.length());

        ScanFilter sf = new ScanFilter(realPrefix.length(), delimiter,
                maxUploads, startKey, stopKey);

        Scan scan = new Scan();
        scan.addFamily(COL_FAMILY_M);
        scan.setStartRow(startKey);
        scan.setStopRow(stopKey);
        scan.setFilter(sf);

        List<Result> results = DBUtil.scan(TAB_NAME, scan);

        LOG.info("got result num: " + results.size());

        ArrayList<MultiPartPebble> pebbles = new ArrayList<MultiPartPebble>();
        ArrayList<String> commonPrefixs = new ArrayList<String>();

        String pebbleID = null;
        MultiPartPebble pebble = null;

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

        return new Pair<ArrayList<MultiPartPebble>, ArrayList<String>>(pebbles,
                commonPrefixs);
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
        assert (rowKey.length > ROW_KEY_SUBFIX_LEN);
        int pos = rowKey.length - ROW_KEY_SUBFIX_LEN;
        p.setPebbleID(Bytes.toString(rowKey, 0, pos));
        // System.out.println(rowKey.length+" "+Bytes.toLong(rowKey,pos+1));
        p.setVersion(Long.MAX_VALUE - Bytes.toLong(rowKey, pos + 1));
    }

    public static Pair<String, Long> parseRowKey(byte[] rowKey) {
        assert (rowKey.length > ROW_KEY_SUBFIX_LEN);
        int pos = rowKey.length - ROW_KEY_SUBFIX_LEN;

        Pair<String, Long> p = new Pair<String, Long>();
        p.setFirst(Bytes.toString(rowKey, 0, pos));
        p.setSecond(Long.MAX_VALUE - Bytes.toLong(rowKey, pos + 1));
        return p;
    }

    public static void parseRowKey(byte[] buffer, int offset, int length,
            Pebble p) {
        assert (length > ROW_KEY_SUBFIX_LEN);
        int pos = length - ROW_KEY_SUBFIX_LEN;

        p.setPebbleID(Bytes.toString(buffer, offset, pos));
        p.setVersion(Long.MAX_VALUE - Bytes.toLong(buffer, offset + pos + 1));
    }

    public static Pair<String, Long> parseRowKey(byte[] buffer, int offset,
            int length) {
        assert (length > ROW_KEY_SUBFIX_LEN);
        int pos = length - ROW_KEY_SUBFIX_LEN;

        Pair<String, Long> p = new Pair<String, Long>();
        p.setFirst(Bytes.toString(buffer, offset, pos));
        p.setSecond(Long.MAX_VALUE - Bytes.toLong(buffer, offset + pos + 1));
        return p;
    }

    public static void main(String[] args) {
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
