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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

public class ScanFilter extends FilterBase {

    public static Logger LOG = Logger.getLogger(ScanFilter.class);

    private static final long DELETE_MARK_VERSION_VAL = Long.MAX_VALUE;
    public static final int ROW_KEY_SUFFIX_LEN = 9;
    private static final byte[] START_TAIL_BYTES = new byte[] { 0 };
    private static final byte[] STOP_TAIL_BYTES = new byte[] { (byte) 0x0ff };

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

        // if pebble is deleted, ignore all version of this pebble
        if (version == DELETE_MARK_VERSION_VAL) {
            dropRow = true;
            skipEndKey = makeStartKey(rowKey);
            LOG.debug("skip deleted whole row: " + rowKey);
            return true;
        }

        // check delimiter
        // not filter delimiter
        if (delimiter.isEmpty()) {
            --maxKeys;
            // only fetch one version per pebble
            skipEndKey = makeStartKey(rowKey);
        } else {
            pos = rowKey.indexOf(delimiter, prefixLen);
            if (pos < 0) {
                --maxKeys;
                skipEndKey = makeStartKey(rowKey);
            } else {
                // common prefix, only reserver one
                skipEndKey = makeStopKey(rowKey
                        .substring(0, pos + delimiterLen));

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
                        - PebbleDB.ROW_KEY_SUFFIX_LEN));

        // skip common prefix
        if (skipEndKey != null
                && Bytes.compareTo(buffer, offset, length
                        - PebbleDB.ROW_KEY_SUFFIX_LEN, skipEndKey, 0,
                        skipEndKey.length) <= 0) {
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

    public static Pair<String, Long> parseRowKey(byte[] buffer, int offset,
            int length) {
        assert (length > ROW_KEY_SUFFIX_LEN);
        int pos = length - ROW_KEY_SUFFIX_LEN;

        Pair<String, Long> p = new Pair<String, Long>();
        p.setFirst(Bytes.toString(buffer, offset, pos));
        p.setSecond(Long.MAX_VALUE - Bytes.toLong(buffer, offset + pos + 1));
        return p;
    }

    public static byte[] makeStartKey(String key) {
        return makeStartKey(Bytes.toBytes(key));
    }

    public static byte[] makeStartKey(byte[] key) {
        return Bytes.add(key, START_TAIL_BYTES);
    }

    public static byte[] makeStopKey(String key) {
        return makeStopKey(Bytes.toBytes(key));
    }

    public static byte[] makeStopKey(byte[] key) {
        return Bytes.add(key, STOP_TAIL_BYTES);
    }

}
