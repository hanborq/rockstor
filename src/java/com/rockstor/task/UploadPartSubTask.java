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
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletInputStream;

import org.apache.log4j.Logger;

import com.rockstor.core.io.RockWriter;
import com.rockstor.core.io.RockWriterPool;
import com.rockstor.core.meta.Chunk;
import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.req.Req;

public class UploadPartSubTask<T extends Req> extends StateTask<T> {
    private static Logger LOG = Logger.getLogger(UploadPartSubTask.class);
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);
    private StateTask<T> parentTask = null;
    private Exception exception = null;
    private ServletInputStream input = null;
    private int bufLen = 0;
    private long leftBytes = 0;
    private static final int BIG_BLOCK_SIZE = 4 << 20; // 4M
    private ArrayList<Chunk> chunks = new ArrayList<Chunk>();
    private int allocTimes = 0;
    private static int maxAllocatorTimes = 3;
    private static final long expectedDelay = 120000L;
    private byte[] chunkPrefix = null;
    private short partId = 0;
    private short seqId = 0;
    private Chunk chunk = null;
    private static RockWriterPool rwp = RockWriterPool.getInstance();
    private RockWriter writer = null;
    private long ts = 0;
    private long totalWriteSize = 0;

    @SuppressWarnings("unchecked")
    public UploadPartSubTask(StateTask<T> parentTask, ServletInputStream input,
            byte[] chunkPrefix, short partId, long ts) {
        super(String.format("%s_%d", "UploadPartSubTask",
                SUB_ID_S.incrementAndGet()), null, null, (T) parentTask
                .getReq());
        this.parentTask = parentTask;
        leftBytes = req.getContentLength();
        bufLen = (int) ((leftBytes > BIG_BLOCK_SIZE) ? BIG_BLOCK_SIZE
                : leftBytes);
        this.input = input;
        this.chunkPrefix = chunkPrefix;
        this.partId = partId;
        this.ts = ts;
    }

    public long getWriteSize() {
        return totalWriteSize;
    }

    public ArrayList<Chunk> getChunks() {
        return chunks;
    }

    public Exception getException() {
        return exception;
    }

    @Override
    public void deliverInter() {
        setExpireDelay(expectedDelay); // each read/write, 2 minutes
        setState(StateEnum.READ_HTTP);
    }

    @Override
    public void timeout() throws Exception {
        exception = new ProcessException(StatusCode.ERR408_TIMEOUT);
        complete(exception);
    }

    @Override
    public void exception(Exception e) {
        exception = e;
        complete(exception);
    }

    public void setException(Exception e) {
        this.exception = e;
    }

    @Override
    protected void complete(Exception e) {
        super.complete(e);
        parentTask.setExpireTime(System.currentTimeMillis() + expectedDelay);
        parentTask.setState(StateEnum.WRITE_META);
    }

    @Override
    public void readHttp() throws Exception {
        if (buf == null) {
            ++allocTimes;
            buf = allocator.allocate(bufLen);

            if (buf == null) { // if allocate memory failed, re-deliver it to
                               // read http thread, unless allocate ok or
                               // timeout
                if (allocTimes >= maxAllocatorTimes) {
                    timeout();
                } else {
                    setState(StateEnum.READ_HTTP);
                }
                return;
            }

            buf.setLen(bufLen);
        }

        try {
            int readLen = buf.read(input);
            if (readLen > 0) {
                LOG.info(name + " read http data size: " + readLen + ", buf="
                        + buf.toString());
            }
        } catch (Exception e) {
            throw new ProcessException(StatusCode.ERR400_IncompleteBody);
        }

        if (buf.isFull()) {
            setExpireTime(Long.MAX_VALUE);
            setState(StateEnum.WRITE_CHUNK);
            return;
        }

        setState(StateEnum.READ_HTTP);
    }

    @Override
    public void writeChunk() throws Exception {
        assert (buf != null);

        writer = rwp.get();
        if (writer == null) {
            throw new IOException("allocate Rock Writer Failed!");
        }

        chunk = new Chunk();
        chunk.setChunkPrefix(chunkPrefix);
        chunk.setPartID(partId);
        chunk.setSeqID(seqId++);
        chunk.setSize(buf.getLen());
        chunk.setTimestamp(this.ts);

        try {
            totalWriteSize += chunk.getSize() + Chunk.HEADER_LEN;
            writer.addChunk(chunk, buf);
            rwp.release(writer);
        } catch (Exception e) {
            writer.close();
            throw e;
        } finally {
            chunks.add(chunk);
        }

        // modify offset
        leftBytes -= buf.getLen();

        // write completely ok
        if (leftBytes <= 0) {
            complete();
        } else {
            // partly write ok, change to read state to read more
            buf.reset((int) ((leftBytes > bufLen) ? bufLen : leftBytes));
            setDelayAfterLastExec(expectedDelay);
            setState(StateEnum.READ_HTTP);
        }
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_UploadPartSub");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
