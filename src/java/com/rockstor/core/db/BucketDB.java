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
import java.security.NoSuchAlgorithmException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.rockstor.core.meta.ACL;
import com.rockstor.core.meta.Bucket;
import com.rockstor.util.HBaseClient;

/**
 * BucketDB is a hbase tool used to get/put/query bucket meta data.
 * 
 * @author terry
 * 
 */
public class BucketDB {
    public static Logger LOG = Logger.getLogger(BucketDB.class);
    public static final String TAB_NAME = HBaseClient.TABLE_BUCKET;
    public static final byte[] COL_FAMILY_M = Bytes.toBytes("M");
    public static final byte[] QUALIFER_ETAG = Bytes.toBytes("etag");
    public static final byte[] QUALIFER_OWNER = Bytes.toBytes("owner");
    public static final byte[] QUALIFER_ACL = Bytes.toBytes("acl");
    public static final byte[] QUALIFER_META = Bytes.toBytes("meta");

    public static void create(Bucket bucket) throws NoSuchAlgorithmException,
            IOException {
        bucket.setEtag();
        Put put = new Put(Bytes.toBytes(bucket.getId()));
        put.add(COL_FAMILY_M, QUALIFER_ETAG, bucket.getEtag());
        put.add(COL_FAMILY_M, QUALIFER_OWNER, Bytes.toBytes(bucket.getOwner()));

        // acl
        ACL acl = bucket.getAcl();
        byte[] aclBytes = null;
        if (acl != null) {
            aclBytes = ACL.toColumnValue(acl);
            if (aclBytes != null) {
                put.add(COL_FAMILY_M, QUALIFER_ACL, aclBytes);
            }
        }

        HBaseClient.put(TAB_NAME, put);
        LOG.info("Add bucket to " + TAB_NAME + ", bucket = " + bucket);

        UserDB.addBucket(bucket.getOwner(), bucket.getId());
        LOG.info("Create bucket in DB OK.");
    }

    // used when modify acl
    public static void update(Bucket bucket) throws NoSuchAlgorithmException,
            IOException {
        bucket.setEtag();
        Put put = new Put(Bytes.toBytes(bucket.getId()));
        put.add(COL_FAMILY_M, QUALIFER_ETAG, bucket.getEtag());

        // acl
        ACL acl = bucket.getAcl();
        byte[] aclBytes = null;
        if (acl != null) {
            aclBytes = ACL.toColumnValue(acl);
            if (aclBytes != null) {
                put.add(COL_FAMILY_M, QUALIFER_ACL, aclBytes);
            }
        }

        HBaseClient.put(TAB_NAME, put);
        LOG.info("Update Acl to " + TAB_NAME + " : bucket = " + bucket.getId()
                + ", acl = " + acl);
    }

    public static void remove(Bucket bucket) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(bucket.getId()));
        delete.deleteFamily(COL_FAMILY_M, System.currentTimeMillis());
        HBaseClient.delete(TAB_NAME, delete);
        LOG.info("Delete bucket from " + TAB_NAME + ", bucket = "
                + bucket.getId());
        UserDB.removeBucket(bucket.getOwner(), bucket.getId());
        LOG.info("Finish Delete bucket from DB.");
    }

    public static Bucket get(String id) throws IOException {
        Get get = new Get(Bytes.toBytes(id));
        get.addFamily(COL_FAMILY_M);

        Result result = HBaseClient.get(TAB_NAME, get);

        if (result == null || result.isEmpty()) {
            LOG.info("Get bucket from " + TAB_NAME
                    + " with result==null, bucket= " + id);
            return null;
        }

        NavigableMap<byte[], byte[]> nm = result.getFamilyMap(COL_FAMILY_M);

        if (nm == null || nm.isEmpty()) {
            LOG.info("Get bucket from " + TAB_NAME
                    + " with empty CF<M>, bucket= " + id);
            return null;
        }

        Bucket bucket = new Bucket(id,
                Bytes.toString(nm.remove(QUALIFER_OWNER)));
        bucket.setEtag(nm.remove(QUALIFER_ETAG));

        if (nm.isEmpty())
            return bucket;

        // acl
        ACL acl = ACL.fromColumnValue(nm.remove(QUALIFER_ACL));
        bucket.setAcl(acl);

        LOG.info("Get bucket from " + TAB_NAME + " OK.");
        return bucket;
    }
}
