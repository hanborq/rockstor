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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.rockstor.core.db.BucketDB;
import com.rockstor.core.db.ChunkDB;
import com.rockstor.core.db.GarbageChunkDB;
import com.rockstor.core.db.PebbleDB;
import com.rockstor.core.db.RockDB;
import com.rockstor.core.io.RockReader;
import com.rockstor.core.io.RockReaderPool;
import com.rockstor.core.io.RockWriter;
import com.rockstor.core.io.RockWriterPool;
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
import com.rockstor.webifc.req.ReqCopyObject;

public class CopyObjectTask extends StateTask<ReqCopyObject> {
    private static Logger LOG = Logger.getLogger(CopyObjectTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);
    private Iterator<Chunk> iter = null;
    private int allocateTimes = 0;
    private static int OBJECT_MAX_KEY_LENGTH = 255;
    private static int maxAllocatorTimes = 10;
    private static RockReaderPool rockReaderPool = RockReaderPool.getInstance();
    private RockReader rockReader = null;
    private static RockWriterPool rockWriterPool = RockWriterPool.getInstance();
    private RockWriter rockWriter = null;
    private Pebble srcPebble = null;
    private Pebble dstPebble = null;
    private List<Chunk> dstChunks = new ArrayList<Chunk>();
    private Chunk srcChunk = null;
    private Chunk dstChunk = null;
    private long version = 0;
    private long totalWriteSize = 0;
    private Exception e = null;

    public CopyObjectTask(ReqCopyObject req, HttpServletResponse servletRsp,
            HttpServletRequest servletReq) {
        super(String.format("%s_%d", "CopyObjectTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        String bucketId = req.getBucket();
        Bucket bucket = BucketDB.get(bucketId);
        if (bucket == null) {
            LOG.error("Copy Object Error: bucket not exist : " + bucket);
            throw new ProcessException(StatusCode.ERR404_NoSuchBucket);
        }

        if (!bucket.canWrite(user)) {
            LOG.error("Copy Object Error: user(" + user
                    + ") dose not have write for bucket(" + bucketId + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        String id = bucketId + req.getObject();

        if (id.length() > OBJECT_MAX_KEY_LENGTH) {
            LOG.error("Copy Object Error: name too large : "
                    + req.getContentLength());
            throw new ProcessException(StatusCode.ERR400_KeyTooLong);
        }

        srcPebble = PebbleDB.get(req.getRockstorCopySource());
        if (srcPebble == null) {
            LOG.error("Copy Object Error: src not exist : "
                    + req.getRockstorCopySource());
            throw new ProcessException(StatusCode.ERR404_NoSuchKey);
        }

        if (!srcPebble.canRead(user)) {
            LOG.error("Copy Object Error: user(" + user
                    + ") dose not have read for object("
                    + req.getRockstorCopySource() + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        String eTagString = MD5HashUtil.hexStringFromBytes(srcPebble.getEtag());

        if (req.getRockstorCopySourceIfMatch() != null
                && req.getRockstorCopySourceIfNoneMatch() != null) {
            LOG.error("Copy Object Error: cannot set both IfMatch & IfNoneMatch");
            throw new ProcessException(StatusCode.ERR412_PreconditionFailed);
        } else if (req.getRockstorCopySourceIfMatch() != null
                && !req.getRockstorCopySourceIfMatch().equals(eTagString)) {
            LOG.error("Copy Object Error: IfMatch = "
                    + req.getRockstorCopySourceIfMatch() + ", etag = "
                    + eTagString);
            throw new ProcessException(StatusCode.ERR412_PreconditionFailed);
        } else if (req.getRockstorCopySourceIfNoneMatch() != null
                && req.getRockstorCopySourceIfMatch().equals(eTagString)) {
            LOG.error("Copy Object Error: IfNoneMatch = "
                    + req.getRockstorCopySourceIfNoneMatch() + ", etag = "
                    + eTagString);
            throw new ProcessException(StatusCode.ERR412_PreconditionFailed);
        }

        if (req.getRockstorCopySourceIfModifiedSince() != 0
                && req.getRockstorCopySourceIfUnmodifiedSince() != 0) {
            LOG.error("Copy Object Error: cannot set both IfModifiedSince & IfUnmodifiedSince");
            throw new ProcessException(StatusCode.ERR412_PreconditionFailed);
        } else if (req.getRockstorCopySourceIfModifiedSince() != 0
                && srcPebble.getVersion() <= req
                        .getRockstorCopySourceIfModifiedSince()) {
            LOG.error("Copy Object Error: IfModifiedSince = "
                    + req.getRockstorCopySourceIfModifiedSince()
                    + ", verstion = " + srcPebble.getVersion());
            throw new ProcessException(StatusCode.ERR412_PreconditionFailed);
        } else if (req.getRockstorCopySourceIfUnmodifiedSince() != 0
                && srcPebble.getVersion() > req
                        .getRockstorCopySourceIfUnmodifiedSince()) {
            LOG.error("Copy Object Error: IfUnmodifiedSince = "
                    + req.getRockstorCopySourceIfUnmodifiedSince()
                    + ", verstion = " + srcPebble.getVersion());
            throw new ProcessException(StatusCode.ERR412_PreconditionFailed);
        }

        LOG.info("Copy Object: src = " + srcPebble.getPebbleID() + ", dst = "
                + id);

        dstPebble = new Pebble(srcPebble);
        dstPebble.setPebbleID(id);
        dstPebble.setOwner(user);
        if (srcPebble.getAcl() == null) {
            dstPebble.setAcl(new ACL());
        }
        if (srcPebble.getMeta() == null) {
            dstPebble.setMeta(new HashMap<String, String>());
        }

        Set<Chunk> srcChunks = ChunkDB.get(srcPebble.getChunkPrefix());

        if (srcChunks == null
        // || srcChunks.size() != srcPebble.getChunkNum()
        ) {
            LOG.error("Copy Object Error: can not find chunks.");
            throw new ProcessException(StatusCode.ERR404_NoSuchKey);
        }

        version = System.currentTimeMillis();
        dstPebble.setSize(srcPebble.getSize());
        dstPebble.setChunkNum(srcPebble.getChunkNum());
        dstPebble.setVersion(version);
        dstPebble.setChunkPrefix(Chunk.genChunkPrefix(dstPebble));

        iter = srcChunks.iterator();
        srcChunk = iter.next();

        rockReader = rockReaderPool.get(MD5HashUtil.hexStringFromBytes(srcChunk
                .getRockID()));

        if (rockReader == null) {
            throw new ProcessException(StatusCode.ERR500_InternalError);
        }
        setExpireTime(Long.MAX_VALUE);
        setState(StateEnum.READ_CHUNK);
    }

    private void releaseRockReader() {
        if (rockReader != null) {
            rockReaderPool.release(rockReader);
            rockReader = null;
        }
    }

    @Override
    public void exception(Exception e) {
        this.e = e;
        setState(StateEnum.WRITE_META);
    }

    @Override
    public void writeMeta() throws Exception {
        if (e != null) { // something is wrong
            RockDB.putGarbage(dstChunks);
            dstPebble.setSize(totalWriteSize);
            GarbageChunkDB.put(dstPebble);
        } else {
            ChunkDB.put(dstChunks);
            PebbleDB.put(dstPebble);
            rsp.setHeader(TAG.ETAG,
                    MD5HashUtil.hexStringFromBytes(dstPebble.getEtag()));

            LOG.info("Copy Object OK: src = " + srcPebble.getPebbleID()
                    + ", dst = " + dstPebble.getPebbleID());
        }

        complete(e);
    }

    @Override
    public void writeChunk() throws Exception {
        dstChunk = new Chunk(dstPebble.getChunkPrefix());
        dstChunk.setPartID(srcChunk.getPartID());
        dstChunk.setSeqID(srcChunk.getSeqID());
        dstChunk.setSize(srcChunk.getSize());
        dstChunk.setTimestamp(version);

        rockWriter = rockWriterPool.get();
        if (rockWriter == null) {
            throw new ProcessException(StatusCode.ERR500_InternalError);
        }

        dstChunks.add(dstChunk);
        try {
            totalWriteSize += dstChunk.getSize() + Chunk.HEADER_LEN;
            rockWriter.addChunk(dstChunk, buf);
            rockWriterPool.release(rockWriter);
        } catch (Exception e) {
            rockWriter.close();
            throw e;
        }

        if (iter.hasNext()) {
            srcChunk = iter.next();
            // if can reuse
            if (buf.getDataLen() >= srcChunk.getSize()) {
                buf.reset((int) srcChunk.getSize());
            } else {
                allocator.release(buf);
                buf = null;
                allocateTimes = 0;
            }

            // should change rock reader
            if (Bytes
                    .compareTo(srcChunk.getRockID(), rockReader.getRockMagic()) != 0) {
                releaseRockReader();
                rockReader = rockReaderPool.get(MD5HashUtil
                        .hexStringFromBytes(srcChunk.getRockID()));

                if (rockReader == null) {
                    throw new ProcessException(StatusCode.ERR500_InternalError);
                }
            }
            setState(StateEnum.READ_CHUNK);
        } else {
            releaseRockReader();
            setState(StateEnum.WRITE_META);
        }
    }

    @Override
    public void readChunk() throws Exception {
        if (buf == null) {
            buf = allocator.allocate((int) srcChunk.getSize());
            ++allocateTimes;
            if (buf == null) {
                if (allocateTimes >= maxAllocatorTimes) {
                    timeout();
                }

                setState(StateEnum.READ_CHUNK);
                return;
            }

            buf.reset((int) srcChunk.getSize());
        }

        try {
            rockReader.readChunk(srcChunk, buf);
        } catch (Exception e) {
            rockReader.close();
            rockReader = null;
            throw e;
        }

        if (buf.isFull()) {
            setState(StateEnum.WRITE_CHUNK);
        } else {
            setState(StateEnum.READ_CHUNK);
        }
    }

    @Override
    protected void complete(Exception e) {
        releaseRockReader();
        super.complete(e);
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_CopyObjectTask");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
