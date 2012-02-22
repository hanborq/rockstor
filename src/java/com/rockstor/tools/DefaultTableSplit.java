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

package com.rockstor.tools;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class DefaultTableSplit implements TableSplitInterface {
    private static Logger LOG = Logger.getLogger(DefaultTableSplit.class);

    private static String[] binarySplitTables = new String[] { "Chunk",
            "GarbageChunk", "Rock" };
    private static String[] charSplitTables = new String[] { "Bucket",
            "Pebble", "MultiPartPebble", "User" };

    public DefaultTableSplit() {
    }

    @Override
    public Map<String, byte[][]> generateSplits(Map<String, byte[][]> oldSplits) {
        LOG.info(DefaultTableSplit.class.getName()
                + " start to generate table splits!");

        if (oldSplits != null && !oldSplits.isEmpty()) {
            return oldSplits;
        }

        Map<String, byte[][]> tbSplitsMap = new TreeMap<String, byte[][]>();

        String chars = new String(
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ023456789");
        byte[][] charSplitKeys = new byte[chars.length()][];
        for (int i = 0; i < chars.length(); ++i) {
            charSplitKeys[i] = Bytes.toBytes(chars.charAt(i));
        }

        byte[][] binSplitKeys = Bytes.split(HConstants.EMPTY_START_ROW,
                Bytes.toBytes(-1), 16);
        binSplitKeys = Arrays.copyOfRange(binSplitKeys, 1, binSplitKeys.length);

        for (String tbName : binarySplitTables) {
            tbSplitsMap.put(tbName, binSplitKeys);
        }

        for (String tbName : charSplitTables) {
            tbSplitsMap.put(tbName, charSplitKeys);
        }

        return tbSplitsMap;
    }

}
