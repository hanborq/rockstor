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

import com.rockstor.core.db.BucketDB;
import com.rockstor.core.db.ChunkDB;
import com.rockstor.core.db.GarbageChunkDB;
import com.rockstor.core.db.PebbleDB;
import com.rockstor.core.db.RockDB;
import com.rockstor.core.meta.ACL;
import com.rockstor.core.meta.Bucket;
import com.rockstor.core.meta.Chunk;
import com.rockstor.core.meta.Pebble;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.MD5HashUtil;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.TAG;
import com.rockstor.webifc.req.ReqPutObject;

public class PutObjectTask extends StateTask<ReqPutObject> {
    private static Logger LOG = Logger.getLogger(PutObjectTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);
    private UploadPartSubTask<ReqPutObject> subTask = null;
    private Pebble pebble = null;
    private static long OBJECT_MIN_LENGTH = 1L;
    private static long OBJECT_MAX_LENGTH = 5 * 1024 * 1024 * 1024L;
    public static int OBJECT_MAX_KEY_LENGTH = 255;

    public PutObjectTask(ReqPutObject req, HttpServletResponse servletRsp,
            HttpServletRequest servletReq) {
        super(String.format("%s_%d", "PutObjectTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        // 1. check bucket acl
        String bucketId = req.getBucket();
        Bucket bucket = BucketDB.get(bucketId);

        if (bucket == null) {
            LOG.error("Put Object Error: bucket not exist : " + bucketId);
            throw new ProcessException(StatusCode.ERR404_NoSuchBucket);
        }

        if (!bucket.canWrite(user)) {
            LOG.error("Put Object Error: user(" + user
                    + ") dose not have write right for bucket(" + bucketId
                    + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        // check chunk size
        if (req.getContentLength() < OBJECT_MIN_LENGTH) {
            LOG.error("Put Object Error: size too small : "
                    + req.getContentLength());
            throw new ProcessException(StatusCode.ERR400_EntityTooSmall);
        }

        if (req.getContentLength() > OBJECT_MAX_LENGTH) {
            LOG.error("Put Object Error: size too large : "
                    + req.getContentLength());
            throw new ProcessException(StatusCode.ERR400_EntityTooLarge);
        }

        String id = bucketId + req.getObject();

        // check id length
        if (id.length() > OBJECT_MAX_KEY_LENGTH) {
            LOG.error("Put Object Error: name too large : " + id.length());
            throw new ProcessException(StatusCode.ERR400_KeyTooLong);
        }

        pebble = new Pebble();
        pebble.setPebbleID(id);
        pebble.setMIME(req.getContentType());
        pebble.setMeta(req.getMetas());
        pebble.setOwner(user);

        String aclString = req.getRockstorAcl();
        if (req.getRockstorAcl() == null) {
            aclString = ACL.PRIVATE;
        }

        try {
            pebble.initACL(aclString, bucket.getOwner());
        } catch (IllegalArgumentException e) {
            LOG.error("Put Object Error: Unknown acl : " + aclString);
            throw new ProcessException(StatusCode.ERR400_UnsupportedAcl);
        }

        long ts = System.currentTimeMillis();
        pebble.setVersion(ts);
        pebble.setChunkPrefix(Chunk.genChunkPrefix(pebble));
        pebble.setChunkNum((short) 1);
        pebble.setSize(req.getContentLength());

        subTask = new UploadPartSubTask<ReqPutObject>(this,
                this.servletReq.getInputStream(), pebble.getChunkPrefix(),
                (short) 1, ts);

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
        pebble.setSize(subTask.getWriteSize());
        GarbageChunkDB.put(pebble);
    }

    private void saveMeta() throws IOException, NoSuchAlgorithmException {
        ChunkDB.put(subTask.getChunks());
        // pebble.setChunkNum((short)subTask.getChunks().size());
        PebbleDB.put(pebble);

        rsp.setHeader(TAG.ETAG,
                MD5HashUtil.hexStringFromBytes(pebble.getEtag()));

        LOG.info("Put Object OK: put pebble: " + pebble.toString()
                + ", Content Type: " + req.getContentType());

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
            .getListener("Task_PutObject");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
