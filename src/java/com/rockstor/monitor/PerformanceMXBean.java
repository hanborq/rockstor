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

package com.rockstor.monitor;

public interface PerformanceMXBean {
    public String getName();

    public long getTotalThroughput();

    public long getTotalMinDelay();

    public long getTotalMaxDelay();

    public long getTotalAvgDelay();

    public long getTotal90Delay();

    public long getCurrentThroughput();

    public long getCurrentMinDelay();

    public long getCurrentMaxDelay();

    public long getCurrentAvgDelay();

    public long getCurrent90Delay();

    public String getTotalStartTimestamp();

    public String getTotalStopTimestamp();

    public String getCurrentStartTimestamp();

    public String getCurrentStopTimestamp();

    public String getCurrentStatus();

    public String getDelayDistribute();

    public long getCurrentSuccNum();

    public long getCurrentFailNum();

    public float getCurrentSuccRate();

    public float getCurrentFailRate();

    public long getCurrentFinishNum();

    public long getCurrentPendNum();

    public long getCurrentDeliverNum();

    public float getCurrentPendRate();

    public float getCurrentFinishRate();

    public long getTotalSuccNum();

    public long getTotalFailNum();

    public float getTotalSuccRate();

    public float getTotalFailRate();

    public long getTotalFinishNum();

    public long getTotalPendNum();

    public long getTotalDeliverNum();

    public float getTotalPendRate();

    public float getTotalFinishRate();
}
