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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.rockstor.core.db.DBJMXFactory;

public class DBUtil {
    private static final int BATCH_FETCH_NUM = 1000;
    private static final byte[] START_TAIL_BYTES = new byte[] { 0 };
    private static final byte[] STOP_TAIL_BYTES = new byte[] { (byte) 0x0ff };
    public static DBJMXFactory dbJMXFactory = DBJMXFactory.getInstance();

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

    public static List<Result> scan(String tab, Scan scan, int num)
            throws IOException {
        return scan(tab, scan, num, null, null);
    }

    public static List<Result> scan(String tab, Scan scan) throws IOException {
        return scan(tab, scan, 0);
    }

    public static List<Result> scan(String tab, Scan scan, int num,
            byte[] startRow) throws IOException {
        return scan(tab, scan, num, startRow, null);
    }

    public static List<Result> scan(String tab, Scan scan, byte[] startRow)
            throws IOException {

        return scan(tab, scan, 0, startRow);
    }

    public static List<Result> scan(String tab, Scan scan, int num,
            byte[] startRow, byte[] stopRow) throws IOException {
        if (startRow != null) {
            scan.setStartRow(startRow);
        }

        if (stopRow != null) {
            scan.setStopRow(stopRow);
        }

        int reqNum = num;
        if (num <= 0) {
            reqNum = Integer.MAX_VALUE;
        }

        int batchNum = BATCH_FETCH_NUM > reqNum ? reqNum : BATCH_FETCH_NUM;
        scan.setCaching(batchNum);
        scan.setMaxVersions();
        DelayTime dt = new DelayTime();
        ResultScanner rs = HBaseClient.getScanner(tab, scan);
        List<Result> results = new LinkedList<Result>();
        Result[] curResults = null;

        int curReqNum = BATCH_FETCH_NUM;
        try {
            do {
                if (reqNum < BATCH_FETCH_NUM) {
                    curReqNum = reqNum;
                }

                curResults = rs.next(curReqNum);
                for (Result r : curResults) {
                    results.add(r);
                }

                reqNum -= curResults.length;
            } while (curResults.length == BATCH_FETCH_NUM && reqNum > 0);
        } catch (IOException e) {
            throw e;
        } finally {
            rs.close();
            dbJMXFactory.getDBJMX(tab).scan(dt.delay());
        }

        return results;
    }

    public static void main(String[] argv) {
        byte[] startKey = makeStartKey(new byte[] { 1, 2 });
        byte[] stopKey = makeStopKey(new byte[] { 1, 2 });
        for (byte b : startKey) {
            System.out.print(b + " ");
        }

        System.out.println();
        for (byte b : stopKey) {
            System.out.print(b + " ");
        }

        System.out.println();

        byte[] key = "abc".getBytes();
        startKey = makeStartKey(key);
        stopKey = makeStopKey(key);

        byte[][] keys = new byte[][] { "abb".getBytes(), // -1 -1
                key, // -1 -1
                startKey, // 0 -1
                "abcd".getBytes(), // 1 -1
                stopKey, // 1 0
                "abd".getBytes() }; // 1 1

        for (byte[] k : keys) {
            System.out.print(Bytes.compareTo(k, startKey) + " ");
        }

        System.out.println();
        for (byte[] k : keys) {
            System.out.print(Bytes.compareTo(k, stopKey) + " ");
        }

    }
}
