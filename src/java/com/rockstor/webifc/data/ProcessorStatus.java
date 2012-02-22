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

package com.rockstor.webifc.data;

public class ProcessorStatus {

    private int pendingTaskNum;
    private int processedTaskNum;
    private int totalWorkerNum;
    private int busyWorkerNum;

    /**
     * @return the processedTaskNum
     */
    public int getProcessedTaskNum() {
        return processedTaskNum;
    }

    /**
     * @param processedTaskNum
     *            the processedTaskNum to set
     */
    public void setProcessedTaskNum(int processedTaskNum) {
        this.processedTaskNum = processedTaskNum;
    }

    /**
     * @return the pendingTaskNum
     */
    public int getPendingTaskNum() {
        return pendingTaskNum;
    }

    /**
     * @param pendingTaskNum
     *            the pendingTaskNum to set
     */
    public void setPendingTaskNum(int pendingTaskNum) {
        this.pendingTaskNum = pendingTaskNum;
    }

    /**
     * @return the totalWorkerNum
     */
    public int getTotalWorkerNum() {
        return totalWorkerNum;
    }

    /**
     * @param totalWorkerNum
     *            the totalWorkerNum to set
     */
    public void setTotalWorkerNum(int totalWorkerNum) {
        this.totalWorkerNum = totalWorkerNum;
    }

    /**
     * @return the busyWorkerNum
     */
    public int getBusyWorkerNum() {
        return busyWorkerNum;
    }

    /**
     * @param busyWorkerNum
     *            the busyWorkerNum to set
     */
    public void setBusyWorkerNum(int busyWorkerNum) {
        this.busyWorkerNum = busyWorkerNum;
    }
}
