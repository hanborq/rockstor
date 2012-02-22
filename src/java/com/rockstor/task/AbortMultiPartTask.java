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

import com.rockstor.core.db.MultiPartPebbleDB;
import com.rockstor.core.meta.MultiPartPebble;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.StatusCode;
import com.rockstor.util.UploadIdGen;
import com.rockstor.webifc.req.ReqAbortMultiPart;

public class AbortMultiPartTask extends StateTask<ReqAbortMultiPart> {
    private static Logger LOG = Logger.getLogger(AbortMultiPartTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);
    private MultiPartPebble pebble = null;
    private String id = null;

    public AbortMultiPartTask(ReqAbortMultiPart req,
            HttpServletResponse servletRsp, HttpServletRequest servletReq) {
        super(String.format("%s_%d", "AbortMultiPartTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        id = req.getBucket() + req.getObject();
        long version = UploadIdGen.uploadID2Ver(id, req.getUploadId());
        pebble = (MultiPartPebble) MultiPartPebbleDB.getPebbleWithVersion(id,
                version);

        if (pebble == null) {
            LOG.error("MultiPart Object Abort Error: object not exist : " + id);
            throw new ProcessException(StatusCode.ERR404_NoSuchKey);
        }

        if (!user.equals(pebble.getOwner())) {
            LOG.error("MultiPart Object Abort Error: user(" + user
                    + ") is not the owner of object(" + id + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        setState(StateEnum.WRITE_META);
    }

    @Override
    public void writeMeta() throws Exception {
        MultiPartPebbleDB.abort(pebble);
        rsp.setStatus(204);
        LOG.info("Put MultiPart Object Abort OK: object = " + id);
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_AbortMultiPart");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
