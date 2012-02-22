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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.rockstor.core.db.BucketDB;
import com.rockstor.core.meta.ACL;
import com.rockstor.core.meta.Bucket;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.RockConfiguration;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.req.ReqPutBucket;

public class PutBucketTask extends StateTask<ReqPutBucket> {
    private static Logger LOG = Logger.getLogger(PutBucketTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);
    private static Configuration conf = RockConfiguration.getDefault();
    private static Pattern bucketNamePattern = Pattern
            .compile(conf.get("rock.bucket.name.regex",
                    "[A-Za-z]{1}[A-Za-z0-9_]{0,63}"), Pattern.CASE_INSENSITIVE);
    private String bucketID = null;

    public PutBucketTask(ReqPutBucket req, HttpServletResponse servletRsp,
            HttpServletRequest servletReq) {
        super(String.format("%s_%d", "PutBucketTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        if (user == null) {
            LOG.error("Put Bucket Error: User is null.");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }
        bucketID = req.getBucket();
        Bucket bucket = BucketDB.get(bucketID);
        if (bucket != null) {
            if (user.equals(bucket.getOwner())) {
                LOG.error("Put Bucket Error: Already is the owner of bucket "
                        + bucketID);
                throw new ProcessException(
                        StatusCode.ERR409_BucketAlreadyOwnedByYou);
            }
            LOG.error("Put Bucket Error: Already exist bucket " + bucketID);
            throw new ProcessException(StatusCode.ERR409_BucketNameUnavailable);
        }

        if (!bucketNamePattern.matcher(bucketID).matches()) {
            LOG.error("the bucket name dose not match patten{"
                    + bucketNamePattern.pattern() + "} : " + bucketID);
            throw new ProcessException(StatusCode.ERR400_InvalidBucketName);
        }

        setState(StateEnum.WRITE_META);
    }

    @Override
    public void writeMeta() throws Exception {
        String aclString = req.getRockstorAcl();
        if (aclString == null) {
            aclString = ACL.PRIVATE;
        }

        Bucket bucket = new Bucket();
        try {
            bucket.initACL(aclString);
        } catch (IllegalArgumentException e) {
            LOG.error("Put Bucket Error: Unknown acl : " + aclString);
            throw new ProcessException(StatusCode.ERR400_UnsupportedAcl);
        }
        bucket.setId(bucketID);
        bucket.setOwner(user);

        BucketDB.create(bucket);
        LOG.info("Put Bucket OK, bucket = " + bucket);
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_PutBucket");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
