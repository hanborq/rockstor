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

package com.rockstor.core.meta;

import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.rockstor.core.db.MultiPartPebbleDB;
import com.rockstor.util.MD5HashUtil;

public class MultiPartPebble extends Pebble {
    public static class PartInfo {
        public static Random RANDOM_GEN = new Random();
        static {
            RANDOM_GEN.setSeed(System.currentTimeMillis());
        }
        public static int ETAG_LEN = 16;

        public static byte[] genRandomEtag() {
            return Bytes.add(Bytes.toBytes(RANDOM_GEN.nextLong()),
                    Bytes.toBytes(RANDOM_GEN.nextLong()));
        }

        public short partId = -1;
        public long size = -1;
        public long timestamp = -1;
        public byte[] etag = null;

        public PartInfo(short partId, long size, byte[] etag) {
            this.partId = partId;
            this.size = size;
            this.etag = etag;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public PartInfo(short partId, long size) {
            this(partId, size, genRandomEtag());
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }

            if (o instanceof PartInfo) {
                PartInfo p = (PartInfo) o;
                return p.partId == this.partId && p.size == this.size
                        && Bytes.equals(p.etag, this.etag)
                        && p.timestamp == this.timestamp;
            }

            return false;
        }

        @Override
        public String toString() {
            return "[Part: Id="
                    + partId
                    + ", size="
                    + size
                    + ", etag="
                    + ((etag == null) ? etag : MD5HashUtil
                            .hexStringFromBytes(etag)) + ", timestamp="
                    + timestamp + "]";
        }

        public static byte[] toColKey(PartInfo pi) {
            return Bytes.toBytes(MultiPartPebbleDB.PART_PREFIX + pi.partId);
        }

        public static byte[] toColValue(PartInfo pi) {
            return Bytes.add(Bytes.toBytes(pi.size),
                    Bytes.toBytes(pi.timestamp), pi.etag);
        }

        public static PartInfo toPartInfo(byte[] colKey, byte[] colValue) {
            short partId = -1;
            String keyStr = Bytes.toString(colKey);

            if (keyStr.length() < MultiPartPebbleDB.PART_PREFIX_LEN) {
                return null;
            }

            partId = Short.parseShort(keyStr
                    .substring(MultiPartPebbleDB.PART_PREFIX_LEN));
            PartInfo partInfo = new PartInfo(partId, Bytes.toLong(Bytes.head(
                    colValue, Bytes.SIZEOF_LONG)), Bytes.tail(colValue,
                    ETAG_LEN));
            partInfo.setTimestamp(Bytes.toLong(colValue, Bytes.SIZEOF_LONG,
                    Bytes.SIZEOF_LONG));
            return partInfo;
        }
    }

    private TreeMap<Short, PartInfo> parts = new TreeMap<Short, PartInfo>();

    /**
     * 
     * @param offset
     */
    public MultiPartPebble(long offset) {
        super("", 0, 0);
    }

    public MultiPartPebble() {
        super("", 0, 0);
    }

    /**
     * constructor
     * 
     * @param pebbleID
     */
    public MultiPartPebble(String pebbleID) {
        super(pebbleID, 0, 0);
    }

    /**
     * constructor
     * 
     * @param pebbleID
     * @param version
     */
    public MultiPartPebble(String pebbleID, long version) {
        super(pebbleID, 0, version);
    }

    /**
     * constructor
     * 
     * @param pebbleID
     * @param rockID
     * @param size
     * @param offset
     */
    public MultiPartPebble(String pebbleID, byte[] rockID, long size,
            long offset) {
        super(pebbleID, size, System.currentTimeMillis());
    }

    /**
     * 
     * @param pebbleID
     * @param size
     * @param offset
     * @param version
     */
    public MultiPartPebble(String pebbleID, long size, long version) {
        super(pebbleID, size, version);
    }

    public MultiPartPebble(Pebble pebble) {
        super(pebble);
    }

    /**
     * @param parts
     *            the parts to set
     */
    public void setParts(TreeMap<Short, PartInfo> parts) {
        this.parts = parts;
    }

    /**
     * @return the parts
     */
    public TreeMap<Short, PartInfo> getParts() {
        return parts;
    }

    public void addPart(PartInfo pi) {
        parts.put(pi.partId, pi);
    }

    public Pebble toPebble() throws NoSuchAlgorithmException {
        Pebble pebble = new Pebble(this);
        pebble.setChunkNum((short) parts.size());
        long pebbleSize = 0;
        // calc size
        for (PartInfo pi : parts.values()) {
            pebbleSize += pi.size;
        }
        pebble.setChunkPrefix(Chunk.genChunkPrefix(pebble));
        pebble.setSize(pebbleSize);
        return pebble;
    }

    public static void main(String[] args) {
        PartInfo pi = new PartInfo((short) -1, 12);
        byte[] colKey = PartInfo.toColKey(pi);
        byte[] colValue = PartInfo.toColValue(pi);

        PartInfo pi2 = PartInfo.toPartInfo(colKey, colValue);

        System.out.println(pi == pi2);
        System.out.println(pi.equals(pi2));
        System.out.println(pi);
        System.out.println(pi2);

    }
}
