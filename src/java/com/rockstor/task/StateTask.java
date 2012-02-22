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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.eclipse.jetty.continuation.Continuation;
import org.eclipse.jetty.continuation.ContinuationSupport;

import com.rockstor.exception.ProcessException;
import com.rockstor.memory.ByteBuffer;
import com.rockstor.memory.MemAllocatorFactory;
import com.rockstor.memory.MemAllocatorInterface;
import com.rockstor.monitor.Listener;
import com.rockstor.state.BaseState;
import com.rockstor.state.BaseStateFactory;
import com.rockstor.state.StateEnum;
import com.rockstor.util.ExceptionLogger;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.auth.AuthException;
import com.rockstor.webifc.auth.AuthTool;
import com.rockstor.webifc.req.Req;

public abstract class StateTask<ReqType extends Req> extends Task {
    private static Logger LOG = Logger.getLogger(StateTask.class);
    private StateEnum stateCode = StateEnum.UNDEFINED;
    private BaseState state = null;
    protected ReqType req = null;
    protected HttpServletRequest servletReq = null;
    protected HttpServletResponse rsp = null;
    protected Continuation continuation = null;
    private static BaseStateFactory stateFactory = BaseStateFactory
            .getInstance();
    protected String user = null;
    protected static AuthTool authPool = null;
    protected ByteBuffer buf = null;
    protected static MemAllocatorInterface allocator = MemAllocatorFactory
            .getInstance().getAllocator();
    protected Listener listener = null;

    static {
        try {
            authPool = AuthTool.getInstance();
        } catch (AuthException e) {
            ExceptionLogger.log(LOG, "get Auth Tool Instance Failed", e);
            System.exit(-1);
        }
    }

    public StateTask(HttpServletRequest servletReq,
            HttpServletResponse servletRsp, ReqType req) {
        this(null, servletReq, servletRsp, req);
    }

    public abstract Listener getListener();

    public StateTask(String name, HttpServletRequest servletReq,
            HttpServletResponse servletRsp, ReqType req) {
        super(name);
        this.servletReq = servletReq;
        this.rsp = servletRsp;
        this.req = req;

        listener = getListener();
        LOG.info(name + " created");
    }

    public final String simpleInfo() {
        return String.format("[%s: State=%s]", name, stateCode.toString());
    }

    @Override
    public final String toString() {
        return String.format("[%s: State=%s, req=<%s>]", name,
                stateCode.toString(), req.toString());
    }

    public final String getUser() {
        return user;
    }

    public void deliver() {
        if (this.rsp != null) {
            this.continuation = ContinuationSupport
                    .getContinuation(this.servletReq);
            this.continuation.setTimeout(-1L);
            this.continuation.suspend();
        }
        LOG.info(toString() + " delivered");
        listener.deliver();
        deliverInter();
    }

    public abstract void deliverInter();

    public final boolean auth() {
        try {
            user = authPool.auth(req);
            return true;
        } catch (AuthException e) {
            LOG.error("Auth error : " + e);
            return false;
        }
    }

    public void exception(Exception e) {
        complete(e);
        // ExceptionLogger.log(LOG,toString()+" Failed", e);
    }

    private final void unsupport() throws Exception {
        throw new ProcessException(StatusCode.ERR500_InternalError);
    }

    protected void complete(Exception e) {
        allocator.release(buf);
        buf = null;

        if (e != null) {
            String msg = (e instanceof ProcessException) ? e.getMessage()
                    : StatusCode.ERR500_InternalError;
            try {
                StatusCode.sendErrorStatusCode(msg, rsp);
            } catch (Exception ex) {

            }
            listener.fail();
            ExceptionLogger.log(LOG, this + " failed", e);
        } else {
            listener.succ(System.currentTimeMillis() - initTs);
            LOG.info(this + " OK!");
        }

        try {
            if (continuation != null) {
                servletReq.getInputStream().close();
                rsp.flushBuffer();
                rsp.getOutputStream().close();
                continuation.complete();
                LOG.info(this + " send response ok!");
            }
        } catch (Exception ex) {

        }

        finish();
    }

    protected void complete() {
        complete(null);
    }

    // default timeout handle. in order to clean context, some complex
    // operations should override this method
    public void timeout() throws Exception {
        setExpireTime(Long.MAX_VALUE); // avoid iterator
        throw new ProcessException(StatusCode.ERR408_TIMEOUT);
    }

    public void dispatchReq() throws Exception {
        unsupport();
    }

    public void dispatchRsp() throws Exception {
        unsupport();
    }

    public void readHttp() throws Exception {
        unsupport();
    }

    public void writeHttp() throws Exception {
        unsupport();
    }

    public void readMeta() throws Exception {
        unsupport();
    }

    public void writeMeta() throws Exception {
        unsupport();
    }

    public void readChunk() throws Exception {
        unsupport();
    }

    public void writeChunk() throws Exception {
        unsupport();
    }

    public void setState(StateEnum stateCode) {
        LOG.info(this + " change state From " + this.stateCode + " to "
                + stateCode);
        this.stateCode = stateCode;
        this.state = stateFactory.getState(this.stateCode);
        this.state.attachThread(this);
    }

    @Override
    public final void exec(long ts) {
        assert (state != null);
        if (this.expired(ts)) {
            setState(StateEnum.TIMEOUT);
            return;
        }
        lastExecTs = ts;
        state.exec(this);
    }

    @Override
    public final void exec() {
        exec(System.currentTimeMillis());
    }

    public Req getReq() {
        return req;
    }
}
