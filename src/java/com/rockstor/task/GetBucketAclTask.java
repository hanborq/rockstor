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
import com.rockstor.core.meta.Bucket;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.data.XMLData;
import com.rockstor.webifc.req.ReqGetBucketAcl;

public class GetBucketAclTask extends StateTask<ReqGetBucketAcl> {
    private static Logger LOG = Logger.getLogger(GetBucketAclTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);

    public GetBucketAclTask(ReqGetBucketAcl req,
            HttpServletResponse servletRsp, HttpServletRequest servletReq) {
        super(String.format("%s_%d", "GetBucketAclTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        // 1. get bucket
        String bucketID = req.getBucket();
        Bucket bucketMeta = BucketDB.get(bucketID);

        if (bucketMeta == null) {
            LOG.error("Get Bucket Acl Error: bucket not exist: " + bucketID);
            throw new ProcessException(StatusCode.ERR404_NoSuchBucket);
        }

        // check acl
        if (!bucketMeta.isFullControl(user)) {
            LOG.error("Get Bucket Acl Error: user(" + user
                    + ") does not have full control for bucket(" + bucketID
                    + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        XMLData.serialize(bucketMeta.getAclXml(), rsp.getOutputStream());
        LOG.info("Get Bucket Acl OK: bucket = " + bucketID);
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_GetBucketAcl");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
