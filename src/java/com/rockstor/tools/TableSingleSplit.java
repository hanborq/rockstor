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

import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

public class TableSingleSplit implements TableSplitInterface {
    private static Logger LOG = Logger.getLogger(TableSingleSplit.class);

    private static String[] tbs = new String[] { "Bucket", "Chunk",
            "GarbageChunk", "MultiPartPebble", "Pebble", "Rock", "User" };

    public TableSingleSplit() {
    }

    @Override
    public Map<String, byte[][]> generateSplits(Map<String, byte[][]> oldSplits) {
        LOG.info(TableSingleSplit.class.getName()
                + " start to generate table splits!");

        if (oldSplits != null && !oldSplits.isEmpty()) {
            return oldSplits;
        }

        Map<String, byte[][]> tbSplitsMap = new TreeMap<String, byte[][]>();

        byte[][] singleSplit = new byte[0][0];
        for (String tbName : tbs) {
            tbSplitsMap.put(tbName, singleSplit);
        }

        return tbSplitsMap;
    }

}
