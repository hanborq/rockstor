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
import com.rockstor.core.meta.Bucket;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.req.ReqDelObject;

public class DelObjectTask extends StateTask<ReqDelObject> {
    private static Logger LOG = Logger.getLogger(DelObjectTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);
    private String pebbleID = null;

    public DelObjectTask(ReqDelObject req, HttpServletResponse servletRsp,
            HttpServletRequest servletReq) {
        super(String.format("%s_%d", "DelObjectTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        String bucketID = req.getBucket();
        pebbleID = bucketID + req.getObject();

        // check bucket acl
        Bucket bucketMeta = BucketDB.get(bucketID);
        if (bucketMeta == null) {
            LOG.error("Del Object Error: bucket not exist: " + bucketID);
            throw new ProcessException(StatusCode.ERR404_NoSuchBucket);
        }

        if (!bucketMeta.canWrite(user)) {
            LOG.error("Del Object Error: user(" + user
                    + ") does not have write right for bucket(" + bucketID
                    + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        setState(StateEnum.WRITE_META);
    }

    @Override
    public void writeMeta() throws Exception {
        // check pebble
        if (!PebbleDB.remove(pebbleID)) {
            LOG.error("Del Object Error: object not exist: " + pebbleID);
            throw new ProcessException(StatusCode.ERR404_NoSuchKey);
        }

        LOG.info("Del Object OK: object = " + pebbleID);
        rsp.setStatus(204);
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_DelObject");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
