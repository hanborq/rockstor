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

import java.sql.Timestamp;

public class PerformanceRecord {
    private long startTs = 0;
    private long stopTs = 0;
    private long failNum = 0;
    private long deliverNum = 0;
    private DelayCounter dc = null;
    private long interval = 0;
    private String startTsStr = null;

    public PerformanceRecord(long startTs, long stopTs, long tsUnit) {
        this.startTs = startTs;
        this.stopTs = stopTs;
        interval = stopTs - startTs;
        dc = new DelayCounter(tsUnit);
        startTsStr = new Timestamp(startTs).toString();
    }

    public PerformanceRecord(long interval) {
        this(interval, System.currentTimeMillis(), 20L);
    }

    public long getEndTs() {
        return stopTs;
    }

    public long getMinDelay() {
        return dc.getMinDelay();
    }

    public long getMaxDelay() {
        return dc.getMaxDelay();
    }

    public long getAvgDelay() {
        return dc.getAvgDelay();
    }

    public long getTotalDelay() {
        return dc.getTotalDelay();
    }

    public long getStartTs() {
        return startTs;
    }

    public boolean belongs(long ts) {
        return ts >= startTs && ts < stopTs;
    }

    public boolean hit(long ts) {
        return ts == startTs;
    }

    public void deliver() {
        ++deliverNum;
    }

    public void succ(long delay) {
        dc.add(delay);
    }

    public void fail() {
        ++failNum;
    }

    public long getThroughput() {
        long deltaTime = System.currentTimeMillis() - startTs;
        deltaTime = (deltaTime > interval) ? interval : deltaTime;
        deltaTime = (deltaTime > 0) ? deltaTime : 1;
        return dc.getTotalNum() * 1000 / deltaTime;
    }

    @Override
    public String toString() {
        return toString("\r\n", true);
    }

    public String getStartTsStr() {
        return startTsStr;
    }

    public String getEndTsStr() {
        long endTs = System.currentTimeMillis();
        endTs = (endTs > stopTs) ? stopTs : endTs;
        return new Timestamp(endTs).toString();
    }

    public long getSpecDelay(float rate) {
        return dc.getSpecDelay(rate);
    }

    public long getSuccNum() {
        return dc.getTotalNum();
    }

    public long getFailNum() {
        return failNum;
    }

    public float getSuccRate() {
        long total = failNum + dc.getTotalNum();
        return (total > 0) ? ((dc.getTotalNum() * 100.0f) / total) : 0.0f;
    }

    public float getFailRate() {
        long total = failNum + dc.getTotalNum();
        return (total > 0) ? ((failNum * 100.0f) / total) : 0.0f;
    }

    public long getFinishNum() {
        return failNum + dc.getTotalNum();
    }

    public long getPendNum() {
        return deliverNum - failNum - dc.getTotalNum();
    }

    public long getDeliverNum() {
        return deliverNum;
    }

    public float getPendRate() {
        return (deliverNum > 0) ? (((deliverNum - failNum - dc.getTotalNum()) * 100.0f) / deliverNum)
                : 0.0f;
    }

    public float getFinishRate() {
        return (deliverNum > 0) ? (((failNum + dc.getTotalNum()) * 100.0f) / deliverNum)
                : 0.0f;
    }

    public String toString(String lineSep, boolean detail) {
        StringBuilder sb = new StringBuilder();
        long totalNum = failNum + dc.getTotalNum();
        long appendNum = deliverNum - totalNum;

        float finishRate = 0.0f;
        float appendRate = 0.0f;

        float failRate = 0.0f;
        float succRate = 0.0f;

        if (totalNum > 0) {
            failRate = (failNum * 100.0f) / totalNum;
            succRate = 100.0f - failRate;
        }

        if (deliverNum > 0) {
            finishRate = (totalNum * 100.0f) / deliverNum;
            appendRate = 100.f - finishRate;
        }

        String timeRange = String.format("[%s -> %s]", startTsStr,
                getEndTsStr());

        sb.append("-------- begin of performance at " + timeRange
                + " statistics -----" + lineSep);
        sb.append(String
                .format("Delivered: %d = %d(append: %.2f%%)+%d(finished: %.2f%% = %d(succNum: %.2f%%)+%d(fail: %.2f%%))%s",
                        deliverNum, appendNum, appendRate, totalNum,
                        finishRate, dc.getTotalNum(), succRate, failNum,
                        failRate, lineSep));

        sb.append(String.format("throughput:  %d inserts/sec%s",
                getThroughput(), lineSep));
        if (detail) {
            sb.append(dc.toString(lineSep, detail));
        }
        sb.append("-------- end of performance at " + timeRange
                + " statistics -----" + lineSep + lineSep);

        return sb.toString();
    }

    private static String recordKeyWord = "record";
    private static String recordPrefix = "<" + recordKeyWord + ">";
    private static String recordSuffix = "</" + recordKeyWord + ">";

    private static String summeryItemFormatStr = "<throughput>%d</throughput>"
            + "<delivered>%d</delivered>" + "<append>%d</append>"
            + "<appendRate>%.2f</appendRate>" + "<executed>%d</executed>"
            + "<executedRate>%.2f</executedRate>" + "<succeed>%d</succeed>"
            + "<succeedRate>%.2f</succeedRate>" + "<failed>%d</failed>"
            + "<failedRate>%.2f</failedRate>";

    private static String timeRangeItemFormat = "<time><from>%s</from><to>%s</to></time>";

    public String getTimeRangeItem() {
        return String.format(timeRangeItemFormat, startTsStr, getEndTsStr());
    }

    public String getSummaryItem() {
        StringBuilder sb = new StringBuilder();

        long totalNum = failNum + dc.getTotalNum();
        long appendNum = deliverNum - totalNum;

        float finishRate = 0.0f;
        float appendRate = 0.0f;

        float failRate = 0.0f;
        float succRate = 0.0f;

        if (totalNum > 0) {
            failRate = (failNum * 100.0f) / totalNum;
            succRate = 100.0f - failRate;
        }

        if (deliverNum > 0) {
            finishRate = (totalNum * 100.0f) / deliverNum;
            appendRate = 100.f - finishRate;
        }

        sb.append(String.format(summeryItemFormatStr, getThroughput(),
                deliverNum, appendNum, appendRate, totalNum, finishRate,
                dc.getTotalNum(), succRate, failNum, failRate));
        return sb.toString();
    }

    public String getDetailXml() {
        StringBuilder sb = new StringBuilder();
        sb.append(recordPrefix);
        sb.append(getTimeRangeItem());
        sb.append(getSummaryItem());
        sb.append(dc.getDetailXml());
        sb.append(recordSuffix);
        return sb.toString();
    }

    public String getSummaryXml() {
        StringBuilder sb = new StringBuilder();
        sb.append(recordPrefix);
        sb.append(getTimeRangeItem());
        sb.append(getSummaryItem());
        sb.append(dc.getSummaryXml());
        sb.append(recordSuffix);
        return sb.toString();
    }
}
