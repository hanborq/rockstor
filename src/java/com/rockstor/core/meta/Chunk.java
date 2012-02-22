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
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

import com.rockstor.core.db.PebbleDB;
import com.rockstor.util.MD5HashUtil;

public class Chunk implements Comparable<Chunk> {
    private byte[] chunkPrefix = null;
    private short partID = -1;

    private byte[] rockID = null;
    private short seqID = -1;
    private long offset = -1;
    private long size = -1;
    private long timestamp = 0;

    public short getSeqID() {
        return seqID;
    }

    public void setSeqID(short seqID) {
        this.seqID = seqID;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public static byte[] genChunkPrefix(Pebble pebble)
            throws NoSuchAlgorithmException {
        return genChunkPrefix(pebble.getPebbleID(), pebble.getVersion());
    }

    public static Chunk row2Chunk(byte[] rowKey, byte[] colKey, byte[] colValue) {
        Chunk chunk = new Chunk(rowKey);
        chunk.setPartID(Bytes.toShort(Bytes.head(colKey, Bytes.SIZEOF_SHORT)));
        chunk.setSeqID(Bytes.toShort(colKey, Bytes.SIZEOF_SHORT,
                Bytes.SIZEOF_SHORT));
        chunk.setTimestamp(Long.MAX_VALUE
                - Bytes.toLong(Bytes.tail(colKey, Bytes.SIZEOF_LONG)));
        chunk.setRockID(Bytes.head(colValue, Rock.ROCK_ID_LEN));
        chunk.setOffset(Bytes.toLong(colValue, Rock.ROCK_ID_LEN,
                Bytes.SIZEOF_LONG));
        chunk.setSize(Bytes.toLong(colValue, Rock.ROCK_ID_LEN
                + Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG));
        return chunk;
    }

    public static byte[] chunk2RowKey(Chunk chunk) {
        return chunk.getChunkPrefix();
    }

    public static byte[] chunk2ColKey(Chunk chunk) {
        return Bytes.add(Bytes.toBytes(chunk.partID),
                Bytes.toBytes(chunk.seqID),
                Bytes.toBytes(Long.MAX_VALUE - chunk.timestamp));
    }

    public static byte[] chunk2ColValue(Chunk chunk) {
        return Bytes.add(chunk.rockID, Bytes.toBytes(chunk.offset),
                Bytes.toBytes(chunk.size));
    }

    public static byte[] chunk2GbKey(Chunk chunk) {
        return Bytes.add(chunk.rockID, Bytes.toBytes(chunk.offset));
    }

    public static byte[] chunk2GbValue(Chunk chunk) {
        return Bytes.add(
                chunk.chunkPrefix,
                Bytes.toBytes(chunk.size),
                Bytes.add(Bytes.toBytes(chunk.partID),
                        Bytes.toBytes(chunk.seqID),
                        Bytes.toBytes(chunk.timestamp)));
    }

    public static Chunk gbRecord2Chunk(byte[] key, byte[] value) {

        Chunk chunk = new Chunk(Bytes.head(value, Chunk.CHUNK_ID_REPFIX_LEN));
        chunk.setSize(Bytes.toLong(value, Chunk.CHUNK_ID_REPFIX_LEN,
                Bytes.SIZEOF_LONG));
        chunk.setPartID(Bytes.toShort(value, Chunk.CHUNK_ID_REPFIX_LEN
                + Bytes.SIZEOF_LONG, Bytes.SIZEOF_SHORT));
        chunk.setSeqID(Bytes.toShort(value, Chunk.CHUNK_ID_REPFIX_LEN
                + Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT, Bytes.SIZEOF_SHORT));
        chunk.setTimestamp(Bytes.toLong(Bytes.tail(value, Bytes.SIZEOF_LONG)));

        chunk.setRockID(Bytes.head(key, Rock.ROCK_ID_LEN));
        chunk.setOffset(Bytes.toLong(key, Rock.ROCK_ID_LEN, Bytes.SIZEOF_LONG));
        return chunk;
    }

    public static byte[] genChunkPrefix(String pebbleID, long pebbleVersion)
            throws NoSuchAlgorithmException {
        byte[] pebbleIDBytes = Bytes.add(
                Bytes.toBytes(pebbleID),
                Bytes.add(PebbleDB.ROW_KEY_SEP,
                        Bytes.toBytes(Long.MAX_VALUE - pebbleVersion)));
        return MD5HashUtil.hashCodeBytes(pebbleIDBytes);
    }

    public static final int CHUNK_ID_REPFIX_LEN = 16;
    public static final int CHUNK_ID_LEN = 16;
    public static final long HEADER_LEN = Rock.ROCK_ID_LEN // header magic len,
            + CHUNK_ID_REPFIX_LEN // chunkPrefix
            + 2 // partID
            + 8 // size,
            + 8 // timestamp, 50 bytes
            + 2; // seqID 52 bytes

    @Override
    public String toString() {
        // if (!valid()) {
        // return "invalid chunk as chunk id is invalid!";
        // }

        return "[Chunk: pebbleIDMD5="
                + (chunkPrefix == null ? "NULL" : MD5HashUtil
                        .hexStringFromBytes(chunkPrefix))
                + ", partNum="
                + partID
                + ", seqNum="
                + seqID
                + ", timestamp="
                + timestamp
                + ", location=[rock="
                + (rockID == null ? "NULL" : MD5HashUtil
                        .hexStringFromBytes(rockID)) + ", offset=" + offset
                + ", size=" + size + "]";
    }

    public boolean valid() {
        return chunkPrefix != null && chunkPrefix.length == CHUNK_ID_LEN
                && rockID != null && rockID.length == Rock.ROCK_ID_LEN;
        // && offset>0 && size>0 && timestamp>0 && partID>=0;
    }

    public short getPartID() {
        return partID;
    }

    public void setPartID(short partID) {
        this.partID = partID;
    }

    public Chunk() {
        this(null);
    }

    public Chunk(byte[] chunkPrefix) {
        this.chunkPrefix = chunkPrefix;
    }

    public byte[] getChunkPrefix() {
        return chunkPrefix;
    }

    public void setChunkPrefix(byte[] chunkPrefix) {
        this.chunkPrefix = chunkPrefix;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public byte[] getRockID() {
        return rockID;
    }

    public void setRockID(byte[] rockID) {
        this.rockID = rockID;
    }

    @Override
    public int compareTo(Chunk o) {
        if (o == this) {
            return 0;
        }

        int ret = Bytes.compareTo(this.chunkPrefix, o.chunkPrefix);
        if (ret != 0) {
            return ret;
        }

        ret = this.partID - o.partID;

        if (ret != 0) {
            return ret;
        }

        ret = this.seqID - o.seqID;
        if (ret != 0) {
            return ret;
        }

        return (int) (o.timestamp - this.timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof Chunk)) {
            return false;
        }

        Chunk c2 = (Chunk) o;
        return this.compareTo(c2) == 0;
    }

    /**
     * @param args
     * @throws NoSuchAlgorithmException
     */
    public static void main(String[] args) throws NoSuchAlgorithmException {

        System.out.println(Chunk.genChunkPrefix("asdf", 12434).length);
        System.out.println(Chunk.HEADER_LEN);

        Set<Chunk> chunks = new TreeSet<Chunk>();
        Chunk c = null;
        for (short i = 4; i >= 0; i--) {
            c = new Chunk(Chunk.genChunkPrefix("chunk", 12434));
            c.setRockID(Chunk.genChunkPrefix("asddf", 1L));
            c.setOffset(100L);
            c.setSize(1L);
            c.setPartID(i);
            c.setSeqID(i);
            c.setTimestamp(System.currentTimeMillis());
            chunks.add(c);
        }

        for (Chunk c1 : chunks) {
            System.out.println(c1);
        }

        c.setPartID((short) 100);
        c.setSeqID((short) 101);
        byte[] rowKey = c.chunkPrefix;
        byte[] colKey = Chunk.chunk2ColKey(c);
        byte[] colValue = Chunk.chunk2ColValue(c);

        Chunk cc = Chunk.row2Chunk(rowKey, colKey, colValue);
        System.out.println(cc.compareTo(c));
        System.out.println(c);
        System.out.println(cc);

        rowKey = Chunk.chunk2GbKey(c);
        colValue = Chunk.chunk2GbValue(c);

        cc = Chunk.gbRecord2Chunk(rowKey, colValue);
        System.out.println(cc.compareTo(c));
        System.out.println(c);
        System.out.println(cc);
    }

}
