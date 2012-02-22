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
import com.rockstor.core.meta.Pebble;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.req.ReqDeleteMeta;

public class DeleteMetaTask extends StateTask<ReqDeleteMeta> {
    private static Logger LOG = Logger.getLogger(DeleteMetaTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);

    private String bucketId = null;
    private String pebbleId = null;
    private Bucket bucket = null;
    private Pebble pebble = null;

    public DeleteMetaTask(ReqDeleteMeta req, HttpServletResponse servletRsp,
            HttpServletRequest servletReq) {
        super(String.format("%s_%d", "DeleteMetaTask",
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
            LOG.error("Delete Object Meta Error: bucket not exist : "
                    + bucketId);
            throw new ProcessException(StatusCode.ERR404_NoSuchBucket);
        }

        if (pebble == null) {
            LOG.error("Delete Object Meta Error: pebble not exist : "
                    + bucketId + pebbleId);
            throw new ProcessException(StatusCode.ERR404_NoSuchKey);
        }

        if (user == null || !pebble.isFullControl(user)) {
            LOG.error("Delete object Meta Error: user(" + user
                    + ") does not have full control for bucket(" + id + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        setState(StateEnum.WRITE_META);
    }

    @Override
    public void writeMeta() throws Exception {
        PebbleDB.deleteMeta(pebble, req.getMetas());
        StringBuffer sb = new StringBuffer();
        for (String k : req.getMetas()) {
            sb.append(k).append(", ");
            ;
        }
        LOG.info("Delete Pebble Meta OK: pebble = " + pebbleId + ", metaKeys=["
                + sb.toString() + "]");
        rsp.setStatus(204);
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_PutObjectMeta");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
