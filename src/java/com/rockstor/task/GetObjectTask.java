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

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.rockstor.core.db.ChunkDB;
import com.rockstor.core.db.PebbleDB;
import com.rockstor.core.io.RockReader;
import com.rockstor.core.io.RockReaderPool;
import com.rockstor.core.meta.Chunk;
import com.rockstor.core.meta.Pebble;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.DateUtil;
import com.rockstor.util.MD5HashUtil;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.TAG;
import com.rockstor.webifc.req.ReqGetObject;

public class GetObjectTask extends StateTask<ReqGetObject> {
    private static Logger LOG = Logger.getLogger(GetObjectTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);
    private Iterator<Chunk> iter = null;
    private Chunk chunk = null;
    private int allocateTimes = 0;
    private static int maxAllocatorTimes = 10;
    private static RockReaderPool rockReaderPool = RockReaderPool.getInstance();
    private RockReader rockReader = null;
    private ServletOutputStream output = null;

    public GetObjectTask(ReqGetObject req, HttpServletResponse servletRsp,
            HttpServletRequest servletReq) {
        super(String.format("%s_%d", "GetObjectTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        setState(StateEnum.READ_META);
    }

    @Override
    public void readMeta() throws Exception {
        String object = req.getBucket() + req.getObject();

        Pebble pebble = PebbleDB.get(object);
        if (pebble == null) {
            LOG.error("Get Object Error: object not exist: " + object);
            throw new ProcessException(StatusCode.ERR404_NoSuchKey);
        }

        // LOG.info("Get pebble ["+ pebble +"], Request User: "+user);

        if (!pebble.canRead(user)) {
            LOG.error("Get Object Error: user(" + user
                    + ") dose not have the read right for object(" + object
                    + ")");
            throw new ProcessException(StatusCode.ERR403_AccessDenied);
        }

        // check version instead of modify time, as pebble write once
        boolean download = true;
        long reqTs = 0;
        String tsStr = null;
        tsStr = req.getIfModifiedSince();

        if (tsStr != null) {
            reqTs = DateUtil.str2Long(tsStr);
            download = (reqTs < pebble.getVersion());
            if (download) {
                LOG.info("Get Object: IfModifiedSince " + reqTs
                        + ", current version : " + pebble.getVersion());
            } else {
                LOG.error("Get Object Error: IfModifiedSince " + reqTs
                        + ", current version : " + pebble.getVersion());
            }
        } else {
            tsStr = req.getIfUnmodifiedSince();
            if (tsStr != null) {
                reqTs = DateUtil.str2Long(tsStr);
                download = (pebble.getVersion() < reqTs);
                if (download) {
                    LOG.info("Get Object: IfUnmodifiedSince " + reqTs
                            + ", current version : " + pebble.getVersion());
                } else {
                    LOG.error("Get Object Error: IfUnmodifiedSince " + reqTs
                            + ", current version : " + pebble.getVersion());
                }
            }
        }

        if (!download) {
            throw new ProcessException(StatusCode.ERR412_PreconditionFailed);
        }

        // if match etag
        String etagReal = MD5HashUtil.hexStringFromBytes(pebble.getEtag());
        String etagMatch = null;

        etagMatch = req.getIfMatch();
        if (etagMatch != null) { // has if-match
            if (!etagMatch.equals(etagReal)) {
                download = false; // unnecessary to download
            }
            if (download) {
                LOG.info("Get Object: IfMatch " + etagMatch
                        + ", current etag : " + etagReal);
            } else {
                LOG.error("Get Object Error: IfMatch " + etagMatch
                        + ", current etag : " + etagReal);
            }
        } else {
            etagMatch = req.getIfNoneMatch();
            if (etagMatch != null) {
                if (etagMatch.equals(etagReal)) {
                    download = false; // unnecessary to download
                }
                if (download) {
                    LOG.info("Get Object: IfNoneMatch " + etagMatch
                            + ", current etag : " + etagReal);
                } else {
                    LOG.error("Get Object Error: IfNoneMatch " + etagMatch
                            + ", current etag : " + etagReal);
                }
            }
        }

        if (!download) {
            throw new ProcessException(StatusCode.ERR412_PreconditionFailed);
        }

        // download
        rsp.setContentType(pebble.getMIME());
        rsp.setHeader(TAG.ETAG, etagReal);
        rsp.setHeader("Last-Modified", DateUtil.long2Str(pebble.getVersion()));
        rsp.setHeader(TAG.CONTENT_LENGTH, String.valueOf(pebble.getSize()));

        if (req.isFromWeb()) {
            String retName = req.getObject().substring(
                    req.getObject().lastIndexOf("/") + 1);
            retName = new String(retName.getBytes("GBK"), "8859_1");
            rsp.setHeader("Content-disposition", "attachment; filename="
                    + retName);
        }

        Set<Chunk> chunks = ChunkDB.get(pebble.getChunkPrefix());

        if (chunks == null) {
            LOG.error("Get Object Error: can not find chunks.");
            throw new ProcessException(StatusCode.ERR404_NoSuchKey);
        } else {
            for (Chunk c : chunks) {
                LOG.info("Need to Read Chunk: " + c);
            }
        }

        iter = chunks.iterator();
        chunk = iter.next();
        output = rsp.getOutputStream();
        rockReader = rockReaderPool.get(MD5HashUtil.hexStringFromBytes(chunk
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
    public void writeHttp() throws Exception {
        buf.write(output);

        if (iter.hasNext()) {
            chunk = iter.next();
            // if can reuse
            if (buf.getDataLen() >= chunk.getSize()) {
                buf.reset((int) chunk.getSize());
            } else {
                allocator.release(buf);
                buf = null;
                allocateTimes = 0;
            }

            // should change rock reader
            if (Bytes.compareTo(chunk.getRockID(), rockReader.getRockMagic()) != 0) {
                releaseRockReader();
                rockReader = rockReaderPool.get(MD5HashUtil
                        .hexStringFromBytes(chunk.getRockID()));

                if (rockReader == null) {
                    throw new ProcessException(StatusCode.ERR500_InternalError);
                }
            }
            setState(StateEnum.READ_CHUNK);
        } else {
            complete();
        }
    }

    @Override
    public void readChunk() throws Exception {
        if (buf == null) {
            buf = allocator.allocate((int) chunk.getSize());
            ++allocateTimes;
            if (buf == null) {
                if (allocateTimes >= maxAllocatorTimes) {
                    timeout();
                }

                setState(StateEnum.READ_CHUNK);
                return;
            }

            buf.reset((int) chunk.getSize());
        }

        try {
            rockReader.readChunk(chunk, buf);
        } catch (Exception e) {
            rockReader.close();
            rockReader = null;
            throw e;
        }

        if (buf.isFull()) {
            setState(StateEnum.WRITE_HTTP);
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
            .getListener("Task_GetObject");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
