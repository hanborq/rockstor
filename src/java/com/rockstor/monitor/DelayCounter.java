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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

public class DelayCounter {
    private Map<Long, Long> delayMap = new HashMap<Long, Long>();
    private long totalNum = 0;
    private long tsUnit = 10; // 20ms
    private long alignDelta = tsUnit - 1;
    private long totalDelay = 0;
    private long maxDelay = 0;
    private long minDelay = Long.MAX_VALUE;
    private long avgDelay = 0;

    public DelayCounter(long tsUnit) {
        assert (tsUnit > 0);
        this.tsUnit = tsUnit;
        this.alignDelta = this.tsUnit - 1;
    }

    public Map<Long, Long> getAll() {
        return delayMap;
    }

    public void add(long delay) {
        ++totalNum;
        totalDelay += delay;

        avgDelay = totalDelay / totalNum;

        maxDelay = (maxDelay > delay) ? maxDelay : delay;
        minDelay = (minDelay > delay) ? delay : minDelay;

        long d = (delay + alignDelta) / tsUnit;
        Long counter = delayMap.get(d);
        if (counter == null) {
            delayMap.put(d, 1L);
        } else {
            delayMap.put(d, counter.longValue() + 1L);
        }
    }

    public long getAvgDelay() {
        return avgDelay;
    }

    public long getMinDelay() {
        return minDelay;
    }

    public long getMaxDelay() {
        return maxDelay;
    }

    public long getTotalNum() {
        return totalNum;
    }

    public long getTotalDelay() {
        return totalDelay;
    }

    @Override
    public String toString() {
        return toString("\r\n", true);
    }

    public long getSpecDelay(float rate) {
        long key = 0;
        long value = 0;
        float sum = 0;
        float curRate = 0;
        for (Entry<Long, Long> entry : new TreeMap<Long, Long>(delayMap)
                .entrySet()) {
            key = entry.getKey() * tsUnit;
            value = entry.getValue();
            curRate = (value * 100.0f) / totalNum;
            sum += curRate;
            if (sum > 90.0)
                break;
        }
        return key;
    }

    private static String detailKeyWord = "detail";
    private static String detailPrefix = "<" + detailKeyWord + ">";
    private static String detailSuffix = "</" + detailKeyWord + ">";
    private static String detailItemFormatStr = "<item>" + "<delay>%d</delay>"
            + "<count>%d</count>" + "<percent>%.3f</percent>"
            + "<summerRate>%.1f</summerRate>" + "</item>";

    private static String delayKeyWord = "delay";
    private static String delayPrefix = "<" + delayKeyWord + ">";
    private static String delaySuffix = "</" + delayKeyWord + ">";
    private static String summerKeyWord = "summary";
    private static String summerPrefix = "<" + summerKeyWord + ">";
    private static String summerSuffix = "</" + summerKeyWord + ">";
    private static String summeryItemFormatStr = "<item>"
            + "<succNum>%d</succNum>" + "<avgDelay>%d</avgDelay>"
            + "<minDelay>%d</minDelay>" + "<maxDelay>%d</maxDelay>" + "</item>";

    public String getDetail() {
        StringBuilder sb = new StringBuilder();
        sb.append(detailPrefix);
        long key = 0;
        long value = 0;
        float sum = 0;
        float curRate = 0;
        for (Entry<Long, Long> entry : new TreeMap<Long, Long>(delayMap)
                .entrySet()) {
            key = entry.getKey() * tsUnit;
            value = entry.getValue();
            curRate = (value * 100.0f) / totalNum;
            sum += curRate;

            sb.append(String.format(detailItemFormatStr, key, value, curRate,
                    sum));
            if (sum > 90.0)
                break;
        }
        sb.append(detailSuffix);
        return sb.toString();
    }

    public String getSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append(summerPrefix);
        sb.append(String.format(summeryItemFormatStr, totalDelay, avgDelay,
                minDelay, maxDelay));
        sb.append(summerSuffix);
        return sb.toString();
    }

    public String getDetailXml() {
        StringBuilder sb = new StringBuilder();
        sb.append(delayPrefix);
        sb.append(getSummary());
        sb.append(getDetail());
        sb.append(delaySuffix);
        return sb.toString();
    }

    public String getSummaryXml() {
        StringBuilder sb = new StringBuilder();
        sb.append(delayPrefix);
        sb.append(getSummary());
        sb.append(delaySuffix);
        return sb.toString();
    }

    public String toString(String lineSep, boolean detail) {
        StringBuilder sb = new StringBuilder();
        sb.append("-------- begin of delay statistics -----" + lineSep);
        if (detail) {
            sb.append(String.format("%12s %12s %12s %12s%s", "delay(ms)",
                    "count", "percent", "sum percent", lineSep));
            long key = 0;
            long value = 0;
            float sum = 0;
            float curRate = 0;
            for (Entry<Long, Long> entry : new TreeMap<Long, Long>(delayMap)
                    .entrySet()) {
                key = entry.getKey() * tsUnit;
                value = entry.getValue();
                curRate = (value * 100.0f) / totalNum;
                sum += curRate;

                sb.append(String.format("%12d %12d %12.3f %12.1f%s", key,
                        value, curRate, sum, lineSep));
                if (curRate > 90.0)
                    break;
            }

            sb.append("----------" + lineSep);
        }
        sb.append(String.format("%12s : %12d%s", "SuccNum", totalNum, lineSep));
        sb.append(String.format("%12s : %12d ms%s", "SuccDelay", totalDelay,
                lineSep));
        sb.append(String.format("%12s : %12d ms%s", "avgDelay", avgDelay,
                lineSep));
        sb.append(String.format("%12s : %12d ms%s", "minDelay", minDelay,
                lineSep));
        sb.append(String.format("%12s : %12d ms%s", "maxDelay", maxDelay,
                lineSep));
        sb.append("-------- end of delay statistics -----" + lineSep + lineSep);
        return sb.toString();
    }
}
