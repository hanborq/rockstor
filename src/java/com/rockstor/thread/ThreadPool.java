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

package com.rockstor.thread;

import java.util.concurrent.atomic.AtomicInteger;

import com.rockstor.state.StateEnum;

public class ThreadPool {
    private AtomicInteger robinNum = new AtomicInteger(0);
    private BaseThread[] threads = null;
    private int threadNum = 0;
    private StateEnum stateCode = StateEnum.UNDEFINED;

    public ThreadPool(int threadNum, StateEnum stateCode) {
        assert (threadNum > 0);
        this.threadNum = threadNum;
        this.stateCode = stateCode;
        threads = new BaseThread[threadNum];
        for (int i = 0; i < threadNum; ++i) {
            threads[i] = new BaseThread(String.format("Thread_%s_%d",
                    this.stateCode.toString(), i));
        }
    }

    public void startAll() {
        for (int i = 0; i < threadNum; i++) {
            threads[i].start();
        }
    }

    public void joinAll() throws InterruptedException {
        for (int i = 0; i < threadNum; i++) {
            threads[i].join();
        }
    }

    public void stopAll() {
        for (int i = 0; i < threadNum; i++) {
            threads[i].end();
        }
    }

    public BaseThread getThread() {
        return threads[robinNum.incrementAndGet() % threadNum];
    }
}
