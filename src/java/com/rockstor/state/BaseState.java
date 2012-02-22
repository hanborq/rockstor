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

package com.rockstor.state;

import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.task.StateTask;
import com.rockstor.thread.ThreadPool;
import com.rockstor.thread.ThreadPoolFactory;
import com.rockstor.webifc.req.Req;

public abstract class BaseState implements State<StateTask<? extends Req>> {
    private StateEnum stateCode = StateEnum.UNDEFINED;
    private ThreadPool threadPool = null;
    private Listener listener = null;

    public BaseState(StateEnum stateCode) {
        this.stateCode = stateCode;
        threadPool = ThreadPoolFactory.getInstance().getThreadPool(stateCode);
        listener = ListenerFactory.getInstance().getListener(
                "State_" + this.stateCode.toString());
    }

    @Override
    public final void attachThread(StateTask<? extends Req> task) {
        listener.deliver();
        threadPool.getThread().addTask(task);
    }

    @Override
    public final String getStateStr() {
        return stateCode.name();
    }

    @Override
    public final StateEnum getStateCode() {
        return stateCode;
    }

    @Override
    public final void exec(StateTask<? extends Req> task) {
        long ts = System.currentTimeMillis();
        try {
            execInter(task);
            listener.succ(System.currentTimeMillis() - ts);
        } catch (Exception e) {
            task.exception(e);
            listener.fail();
        }
    }

    public abstract void execInter(StateTask<? extends Req> task)
            throws Exception;
}
