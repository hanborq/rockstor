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

package com.rockstor.util;

import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.util.Bytes;

public class UploadIdGen {
    public static final int UPLOAD_ID_STR_LEN = 32;
    public static final int UPLOAD_ID_BYTES_LEN = 16;

    public static long ver2Ver(long version) {
        long ret = version;
        long x = ret & 0xff;
        for (int i = 0; i < 7; ++i) {
            x <<= 8;
            ret ^= x;
        }

        return ret;
    }

    public static String ver2UploadId(String pebbleId, long version)
            throws NoSuchAlgorithmException {
        byte[] pebbleIDHash = MD5HashUtil.hashCodeBytes(pebbleId);

        long highWord = Bytes.toLong(pebbleIDHash, 0, Bytes.SIZEOF_LONG);
        long lowWord = Bytes.toLong(pebbleIDHash, Bytes.SIZEOF_LONG,
                Bytes.SIZEOF_LONG);
        version = ver2Ver(version);

        highWord = version ^ highWord;
        lowWord = version ^ lowWord;

        byte[] mixBytes = Bytes.add(Bytes.toBytes(lowWord),
                Bytes.toBytes(highWord));

        return MD5HashUtil.hexStringFromBytes(mixBytes);
    }

    public static long uploadID2Ver(String pebbleID, String uploadID)
            throws NoSuchAlgorithmException {
        byte[] mixBytes = MD5HashUtil.bytesFromHexString(uploadID);
        long highWord1 = Bytes.toLong(mixBytes, Bytes.SIZEOF_LONG,
                Bytes.SIZEOF_LONG);
        long lowWord1 = Bytes.toLong(mixBytes, 0, Bytes.SIZEOF_LONG);
        byte[] pebbleIDHash = MD5HashUtil.hashCodeBytes(pebbleID);
        long highWord = Bytes.toLong(pebbleIDHash, 0, Bytes.SIZEOF_LONG);
        long lowWord = Bytes.toLong(pebbleIDHash, Bytes.SIZEOF_LONG,
                Bytes.SIZEOF_LONG);
        long version1 = highWord1 ^ highWord;
        long version = lowWord1 ^ lowWord;
        if (version1 != version) {
            return -1;
        }
        version = ver2Ver(version);
        return version;
    }

    /**
     * @param args
     * @throws NoSuchAlgorithmException
     */
    public static void main(String[] args) throws NoSuchAlgorithmException {
        String pebbleID = "pebbleId";
        long version = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            String uploadID = ver2UploadId(pebbleID, version);
            long version1 = uploadID2Ver(pebbleID, uploadID);
            System.out.println("version [" + (version1 - version) + "]: "
                    + version + " " + uploadID);
            ++version;
            // System.out.println("version: "+version+", "+version+" : "+(version1-version));
        }
    }

}
