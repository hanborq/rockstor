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

import com.rockstor.exception.ProcessException;
import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.util.StatusCode;
import com.rockstor.webifc.req.ReqMoveObject;

public class MoveObjectTask extends StateTask<ReqMoveObject> {
    private static AtomicInteger SUB_ID_S = new AtomicInteger(0);

    public MoveObjectTask(ReqMoveObject req, HttpServletResponse servletRsp,
            HttpServletRequest servletReq) {
        super(String.format("%s_%d", "MoveObjectTask",
                SUB_ID_S.incrementAndGet()), servletReq, servletRsp, req);
    }

    @Override
    public void deliverInter() {
        exception(new ProcessException(StatusCode.ERR500_InternalError));
    }

    protected static Listener sListener = ListenerFactory.getInstance()
            .getListener("Task_MoveObject");

    @Override
    public Listener getListener() {
        return sListener;
    }
}
