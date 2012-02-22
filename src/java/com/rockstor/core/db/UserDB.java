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
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.rockstor.core.meta.User;
import com.rockstor.util.HBaseClient;

public class UserDB {
    public static Logger LOG = Logger.getLogger(UserDB.class);
    public static final String TAB_NAME = HBaseClient.TABLE_USER;
    public static final byte[] COL_FAMILY_BUCKET = Bytes.toBytes("M");

    public static User get(String id) throws IOException {
        Get g = new Get(Bytes.toBytes(id));
        g.addFamily(COL_FAMILY_BUCKET);

        Result result = HBaseClient.get(TAB_NAME, g);

        if (result == null || result.isEmpty()) {
            LOG.info("Get User from " + TAB_NAME
                    + " with result==null : user = " + id);
            return null;
        }

        NavigableMap<byte[], byte[]> nm = result
                .getFamilyMap(COL_FAMILY_BUCKET);

        if (nm == null || nm.isEmpty()) {
            LOG.info("Get User from " + TAB_NAME
                    + " with empty CF<M> : user = " + id);
            return null;
        }

        User user = new User(id);
        SortedMap<String, Long> buckets = new TreeMap<String, Long>();
        for (Entry<byte[], byte[]> entry : nm.entrySet()) {
            buckets.put(Bytes.toString(entry.getKey()),
                    Bytes.toLong(entry.getValue()));
        }

        user.setBuckets(buckets);
        LOG.info("Get User from " + TAB_NAME + " with " + buckets.size()
                + " buckets : user = " + id);

        return user;
    }

    public static void addBucket(String userID, String bucketID)
            throws IOException {
        Put put = new Put(Bytes.toBytes(userID));
        put.add(COL_FAMILY_BUCKET, Bytes.toBytes(bucketID),
                Bytes.toBytes(System.currentTimeMillis()));
        HBaseClient.put(TAB_NAME, put);
        LOG.info("Add Bucket to " + TAB_NAME + " : uset = " + userID
                + ", bucket = " + bucketID);
    }

    public static void removeBucket(String userID, String bucketID)
            throws IOException {
        Delete delete = new Delete(Bytes.toBytes(userID));
        delete.deleteColumns(COL_FAMILY_BUCKET, Bytes.toBytes(bucketID),
                System.currentTimeMillis());
        HBaseClient.delete(TAB_NAME, delete);
        LOG.info("Remove Bucket from " + TAB_NAME + " : uset = " + userID
                + ", bucket = " + bucketID);
    }

    public static void removeUser(String id) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(id));
        delete.deleteFamily(COL_FAMILY_BUCKET, System.currentTimeMillis());
        HBaseClient.delete(TAB_NAME, delete);
        LOG.info("Remove User from " + TAB_NAME + " : user = " + id);
    }
}
