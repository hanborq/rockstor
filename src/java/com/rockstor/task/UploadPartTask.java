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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.rockstor.core.db.ChunkDB;
import com.rockstor.core.db.MultiPartPebbleDB;
import com.rockstor.core.db.RockDB;
import com.rockstor.core.meta.MultiPartPebble;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.MD5HashUtil;
import com.rockstor.util.StatusCode;
import com.rockstor.util.UploadIdGen;
import com.rockstor.webifc.TAG;
import com.rockstor.webifc.req.ReqUploadPart;

public class UploadPartTask extends StateTask<ReqUploadPart> {
    private static Logger LOG = Logger.getLogger(UploadPartTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);
    private UploadPartSubTask<ReqUploadPart> subTask = null;
    private MultiPartPebble pebble = null;
    private MultiPartPebble.PartInfo part = null;

    public UploadPartTask(ReqUploadPart req, HttpServletResponse servletRsp,
            HttpServletRequest servletReq) {
        super(String.format("%s_%d", "UploadPartTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        // 1. check if operation is legal.
        String id = req.getBucket() + req.getObject();
        long version = UploadIdGen.uploadID2Ver(id, req.getUploadId());
        pebble = (MultiPartPebble) MultiPartPebbleDB.getPebbleWithVersion(id,
                version);

        if (pebble == null) {
            LOG.error("Put MultiPart Object UploadPart Error: object not exist : "
                    + id);
            throw new ProcessException(StatusCode.ERR404_NoSuchKey);
        }

        // only pebble's owner can modify it.
        if (!user.equals(pebble.getOwner())) {
            LOG.error("Put MultiPart Object UploadPart Error: user(" + user
                    + ") is not the owner of object(" + id + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        // 2. check partNumber. partNumber starts with 1.
        short partNumber = req.getPartNumber();
        if (partNumber <= 0) {
            LOG.error("Put MultiPart Object UploadPart Error: part number error : "
                    + partNumber);
            throw new ProcessException(StatusCode.ERR400_InvalidPart);
        }

        LOG.info("Put MultiPart Object UploadPart: object = " + id
                + ", partNum = " + partNumber);

        long ts = System.currentTimeMillis();
        part = new MultiPartPebble.PartInfo(partNumber, req.getContentLength());
        part.setTimestamp(ts);

        subTask = new UploadPartSubTask<ReqUploadPart>(this,
                this.servletReq.getInputStream(), pebble.getChunkPrefix(),
                (short) partNumber, ts);

        subTask.deliver();
    }

    @Override
    public void writeMeta() throws Exception {
        Exception e = null;
        if (subTask == null) {
            complete(new ProcessException(StatusCode.ERR500_InternalError));
            return;
        }

        // check if any exception
        e = subTask.getException();
        if (e == null) {
            saveMeta();
        } else {
            dropGarbage();
        }

        complete(e);
    }

    private void dropGarbage() throws IOException {
        RockDB.putGarbage(subTask.getChunks());
    }

    private void saveMeta() throws IOException, NoSuchAlgorithmException {
        ChunkDB.put(subTask.getChunks());

        MultiPartPebbleDB.addPart(pebble, part);

        rsp.setHeader(TAG.ETAG, MD5HashUtil.hexStringFromBytes(part.etag));
        LOG.info("Put MultiPart Object UploadPart OK: " + req.toString());
    }

    @Override
    public void timeout() throws Exception {
        if (subTask != null) {
            throw new ProcessException(StatusCode.ERR408_TIMEOUT);
        }

        subTask.setException(new ProcessException(StatusCode.ERR408_TIMEOUT));
        setState(StateEnum.WRITE_META);
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_UploadPart");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
