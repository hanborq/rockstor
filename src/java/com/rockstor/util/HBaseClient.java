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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
//import org.apache.log4j.Logger;

import com.rockstor.core.db.DBJMXFactory;

public class HBaseClient {
    // private static Logger LOG = Logger.getLogger(HBaseClient.class);
    public static final String TABLE_MULTI_PART_PEBBLE = "MultiPartPebble";
    public static final String TABLE_GARBAGE_CHUNK = "GarbageChunk";
    public static final String TABLE_CHUNK = "Chunk";
    public static final String TABLE_PEBBLE = "Pebble";
    public static final String TABLE_ROCK = "Rock";
    public static final String TABLE_BUCKET = "Bucket";
    public static final String TABLE_USER = "User";
    private static Configuration conf = RockConfiguration.getDefault();
    public static HTablePool tablePool = null;
    public static int DEFAULT_MAX_NUM_PRE_TABLE = 10;
    public static ConcurrentHashMap<String, Integer> openedTable = new ConcurrentHashMap<String, Integer>();

    public static DBJMXFactory dbJMXFactory = DBJMXFactory.getInstance();

    public static void init() {
        if (null == tablePool) {
            conf = HBaseConfiguration.create(conf);

            tablePool = new HTablePool(conf, conf.getInt(
                    "rock.hbase.cache.num", DEFAULT_MAX_NUM_PRE_TABLE));

            dbJMXFactory.getDBJMX(TABLE_PEBBLE);
            dbJMXFactory.getDBJMX(TABLE_ROCK);
            dbJMXFactory.getDBJMX(TABLE_BUCKET);
            dbJMXFactory.getDBJMX(TABLE_USER);
        }
    }

    public static void init(Configuration conf, int maxCacheSize) {
        if (null == tablePool) {
            tablePool = new HTablePool(conf, maxCacheSize);
        }
    }

    public static void init(Configuration conf) {
        init(conf, DEFAULT_MAX_NUM_PRE_TABLE);
    }

    public static void close() {
        for (String tabName : openedTable.keySet().toArray(new String[0])) {
            tablePool.closeTablePool(tabName);
        }
    }

    public static boolean checkAndPut(final String tableName, final byte[] row,
            final byte[] family, final byte[] qualifier, final byte[] value,
            final Put put) throws IOException {

        HTableInterface table = null;
        boolean ret = false;

        DelayTime dt = new DelayTime();

        try {
            table = tablePool.getTable(tableName);
            ret = table.checkAndPut(row, family, qualifier, value, put);
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
                openedTable.put(tableName, 0);
            }

            dbJMXFactory.getDBJMX(tableName).put(dt.delay());
        }

        return ret;
    }

    public static void delete(final String tableName, final Delete delete)
            throws IOException {

        HTableInterface table = null;
        DelayTime dt = new DelayTime();
        try {
            table = tablePool.getTable(tableName);
            table.delete(delete);
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
                openedTable.put(tableName, 0);
            }
            dbJMXFactory.getDBJMX(tableName).del(dt.delay());
        }
    }

    public static void delete(final String tableName,
            final ArrayList<Delete> deletes) throws IOException {
        HTableInterface table = null;
        DelayTime dt = new DelayTime();
        try {
            table = tablePool.getTable(tableName);
            table.delete(deletes);
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
                openedTable.put(tableName, 0);
            }
            dbJMXFactory.getDBJMX(tableName).del(dt.delay());
        }
    }

    public static boolean exists(final String tableName, final Get get)
            throws IOException {
        HTableInterface table = null;
        boolean ret = false;
        DelayTime dt = new DelayTime();
        try {
            table = tablePool.getTable(tableName);
            ret = table.exists(get);
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
                openedTable.put(tableName, 0);
            }
            dbJMXFactory.getDBJMX(tableName).get(dt.delay());
        }

        return ret;
    }

    public static Result get(final String tableName, final Get get)
            throws IOException {
        HTableInterface table = null;
        Result ret = null;
        DelayTime dt = new DelayTime();
        try {
            table = tablePool.getTable(tableName);
            ret = table.get(get);
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
                openedTable.put(tableName, 0);
            }
            dbJMXFactory.getDBJMX(tableName).get(dt.delay());
        }

        return ret;
    }

    public static Result getRowOrBefore(final String tableName,
            final byte[] row, final byte[] family) throws IOException {
        HTableInterface table = null;
        Result ret = null;
        DelayTime dt = new DelayTime();
        try {
            table = tablePool.getTable(tableName);
            ret = table.getRowOrBefore(row, family);
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
                openedTable.put(tableName, 0);
            }
            dbJMXFactory.getDBJMX(tableName).get(dt.delay());
        }

        return ret;
    }

    public static ResultScanner getScanner(final String tableName,
            final byte[] family) throws IOException {

        HTableInterface table = null;
        ResultScanner ret = null;

        try {
            table = tablePool.getTable(tableName);
            ret = table.getScanner(family);
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
                openedTable.put(tableName, 0);
            }
        }

        return ret;
    }

    public static ResultScanner getScanner(final String tableName,
            final Scan scan) throws IOException {
        HTableInterface table = null;
        ResultScanner ret = null;

        try {
            table = tablePool.getTable(tableName);
            ret = table.getScanner(scan);
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
                openedTable.put(tableName, 0);
            }
        }

        return ret;
    }

    public static void put(final String tableName, final Put put)
            throws IOException {

        HTableInterface table = null;
        DelayTime dt = new DelayTime();
        try {
            table = tablePool.getTable(tableName);
            table.put(put);
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
                openedTable.put(tableName, 0);
            }
            dbJMXFactory.getDBJMX(tableName).put(dt.delay());
        }
    }

    public static void put(final String tableName, final List<Put> puts)
            throws IOException {

        HTableInterface table = null;
        DelayTime dt = new DelayTime();
        try {
            table = tablePool.getTable(tableName);
            table.put(puts);
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
                openedTable.put(tableName, 0);
            }
            dbJMXFactory.getDBJMX(tableName).put(dt.delay());
        }
    }

    public static RowLock lockRow(final String tableName, final byte[] row)
            throws IOException {

        HTableInterface table = null;
        RowLock ret = null;

        try {
            table = tablePool.getTable(tableName);
            ret = table.lockRow(row);
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
                openedTable.put(tableName, 0);
            }
        }

        return ret;
    }

    public static void unlockRow(final String tableName, final RowLock rl)
            throws IOException {

        HTableInterface table = null;

        try {
            table = tablePool.getTable(tableName);
            table.unlockRow(rl);
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
                openedTable.put(tableName, 0);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "10.24.1.252:60000");
        conf.set("hbase.zookeeper.quorum", "10.24.1.252");
        HTable table = new HTable(conf, "Rock");

        Put put = new Put("test".getBytes());
        put.add("M".getBytes(), "test".getBytes(), "value".getBytes());

        table.put(put);

        table.close();

    }

}
