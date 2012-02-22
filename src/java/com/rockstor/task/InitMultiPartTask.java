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
import com.rockstor.core.db.MultiPartPebbleDB;
import com.rockstor.core.meta.ACL;
import com.rockstor.core.meta.Bucket;
import com.rockstor.core.meta.Chunk;
import com.rockstor.core.meta.MultiPartPebble;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.StatusCode;
import com.rockstor.util.UploadIdGen;
import com.rockstor.webifc.data.InitMultiPartResult;
import com.rockstor.webifc.data.XMLData;
import com.rockstor.webifc.req.ReqInitMultiPart;

public class InitMultiPartTask extends StateTask<ReqInitMultiPart> {
    private static Logger LOG = Logger.getLogger(InitMultiPartTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);
    private String id = null;
    private Bucket bucket = null;

    public InitMultiPartTask(ReqInitMultiPart req,
            HttpServletResponse servletRsp, HttpServletRequest servletReq) {
        super(String.format("%s_%d", "InitMultiPartTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        String bucketId = req.getBucket();
        bucket = BucketDB.get(bucketId);

        if (bucket == null) {
            LOG.error("Put MultiPart Object Init Error: bucket not exist : "
                    + bucketId);
            throw new ProcessException(StatusCode.ERR404_NoSuchBucket);
        }

        if (!bucket.canWrite(user)) {
            LOG.error("Put MultiPart Object Init Error: user(" + user
                    + ") dose not have write right for bucket(" + bucketId
                    + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        id = bucketId + req.getObject();

        if (id.length() > PutObjectTask.OBJECT_MAX_KEY_LENGTH) {
            LOG.error("Put MultiPart Object Init Error: name too large : "
                    + id.length());
            throw new ProcessException(StatusCode.ERR400_KeyTooLong);
        }

        setState(StateEnum.WRITE_META);
    }

    @Override
    public void writeMeta() throws Exception {
        MultiPartPebble pebble = new MultiPartPebble();
        pebble.setPebbleID(id);
        pebble.setMIME(req.getContentType());
        pebble.setMeta(req.getMetas());
        pebble.setOwner(user);
        long version = System.currentTimeMillis();
        pebble.setVersion(System.currentTimeMillis());
        pebble.setChunkPrefix(Chunk.genChunkPrefix(pebble));

        String aclString = req.getRockstorAcl();
        if (req.getRockstorAcl() == null) {
            aclString = ACL.PRIVATE;
        }

        try {
            pebble.initACL(aclString, bucket.getOwner());
        } catch (IllegalArgumentException e) {
            LOG.error("Put MultiPart Object Init Error: Unknown acl : "
                    + aclString);
            throw new ProcessException(StatusCode.ERR400_UnsupportedAcl);
        }

        MultiPartPebbleDB.create(pebble);

        String uploadId = UploadIdGen.ver2UploadId(id, version);

        InitMultiPartResult r = new InitMultiPartResult();
        r.setBucket(req.getBucket());
        r.setKey(req.getObject());
        r.setUploadId(uploadId);

        XMLData.serialize(r, rsp.getOutputStream());
        LOG.info("Put MultiPart Object Init OK: " + pebble.getPebbleID());
        complete();
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_InitMultiPart");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
