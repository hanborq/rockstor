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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.rockstor.core.db.BucketDB;
import com.rockstor.core.db.PebbleDB;
import com.rockstor.core.meta.ACL;
import com.rockstor.core.meta.Bucket;
import com.rockstor.core.meta.Pebble;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.req.ReqPutObjectAcl;

public class PutObjectAclTask extends StateTask<ReqPutObjectAcl> {
    private static Logger LOG = Logger.getLogger(PutObjectAclTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);

    private String bucketId = null;
    private String pebbleId = null;
    private Bucket bucket = null;
    private Pebble pebble = null;

    public PutObjectAclTask(ReqPutObjectAcl req,
            HttpServletResponse servletRsp, HttpServletRequest servletReq) {
        super(String.format("%s_%d", "PutObjectAclTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        bucketId = req.getBucket();
        pebbleId = req.getObject();
        bucket = BucketDB.get(bucketId);
        pebble = PebbleDB.get(bucketId + pebbleId);

        if (bucket == null) {
            LOG.error("Put Object Acl Error: bucket not exist : " + bucketId);
            throw new ProcessException(StatusCode.ERR404_NoSuchBucket);
        }

        if (pebble == null) {
            LOG.error("Put Object Acl Error: pebble not exist : " + bucketId
                    + pebbleId);
            throw new ProcessException(StatusCode.ERR404_NoSuchKey);
        }

        if (user == null || !bucket.isFullControl(user)
                || !pebble.isFullControl(user)) {
            LOG.error("Put Bucket Acl Error: user(" + user
                    + ") does not have full control for bucket(" + id + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        setState(StateEnum.WRITE_META);
    }

    @Override
    public void writeMeta() throws Exception {
        ACL acl = new ACL();
        acl.append(req.getAcl());
        pebble.setAcl(acl);

        PebbleDB.update(pebble);
        LOG.info("Put Pebble Acl OK: pebble = " + pebbleId);
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_PutObjectAcl");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
