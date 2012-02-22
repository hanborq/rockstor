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

package com.rockstor.task;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.rockstor.core.db.BucketDB;
import com.rockstor.core.db.PebbleDB;
import com.rockstor.core.meta.Bucket;
import com.rockstor.core.meta.Pebble;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.req.ReqDelBucket;

public class DelBucketTask extends StateTask<ReqDelBucket> {
    private static Logger LOG = Logger.getLogger(DelBucketTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);
    private String bucketID = null;
    private Bucket bucketMeta = null;

    public DelBucketTask(ReqDelBucket req, HttpServletResponse servletRsp,
            HttpServletRequest servletReq) {
        super(String.format("%s_%d", "DelBucketTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        // 1. get bucket
        bucketID = req.getBucket();
        bucketMeta = BucketDB.get(bucketID);

        if (bucketMeta == null) {
            LOG.error("Del Bucket Error: bucket not exist: " + bucketID);
            throw new ProcessException(StatusCode.ERR404_NoSuchBucket);
        }

        // check acl
        if (!bucketMeta.getOwner().equals(user)) {
            LOG.error("Del Bucket Error: user(" + user
                    + ") is not owner of bucket(" + bucketID + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        // check if bucket is empty
        Pair<ArrayList<Pebble>, ArrayList<String>> p = PebbleDB.scan(bucketID,
                null, 1);
        if (!p.getFirst().isEmpty() && p.getSecond().isEmpty()) {
            LOG.error("Del Bucket Error: bucket not empty: " + bucketID);
            throw new ProcessException(StatusCode.ERR409_BucketNotEmpty);
        }

        setState(StateEnum.WRITE_META);
    }

    @Override
    public void writeMeta() throws Exception {
        BucketDB.remove(bucketMeta);
        LOG.info("Del Bucket OK: bucket = " + bucketID);
        rsp.setStatus(204);
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_DelBucket");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
