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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;

import com.rockstor.Constant;
import com.rockstor.util.DateUtil;
import com.rockstor.util.RockConfiguration;

public class DefaultListener implements Listener, PerformanceMXBean {
    private String name = "unknown";
    private static long interval = 0;
    private static int historyNum = 0;
    private static long tsUnit = 0;
    private static String xmlHeader = "<?xml version=\"1.0\" encoding=\"utf-8\" ?>";
    private PerformanceRecord avgRecord = null;
    private PerformanceRecord curRecord = null;
    private PerformanceRecord oldRecord = null;
    private LinkedList<PerformanceRecord> records = new LinkedList<PerformanceRecord>();
    private LinkedList<String> historys = new LinkedList<String>();
    private TreeMap<Long, PerformanceRecord> recordMap = new TreeMap<Long, PerformanceRecord>();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private ReadLock readLock = lock.readLock();
    private WriteLock writeLock = lock.writeLock();

    private static long defaultInterval = 30000L; // 2 minutes
    private static int defaultHistoryNum = 10;
    private static long defaultTsUnit = 1L; // 10 ms

    static {
        Configuration conf = RockConfiguration.getDefault();
        interval = conf.getLong("rockstor.monitor.interval", defaultInterval);
        historyNum = conf.getInt("rockstor.monitor.historyNum",
                defaultHistoryNum);
        tsUnit = conf.getLong("rockstor.monitor.tsUnit", defaultTsUnit);

        if (interval < defaultInterval) {
            interval = defaultInterval;
        }

        if (tsUnit < 1) {
            tsUnit = defaultTsUnit;
        }

        if (historyNum < 1) {
            historyNum = defaultHistoryNum;
        }
    }

    // alignTs to startTs
    private static long tsPoint() {
        long ts = System.currentTimeMillis();
        return ts - (ts % interval);
    }

    public static final String MBEAN_NAME_PREFIX = "com.rockstor.jmx.performance:type=";

    public DefaultListener(String name) {
        long startTs = System.currentTimeMillis();
        avgRecord = new PerformanceRecord(startTs, Long.MAX_VALUE, tsUnit);
        this.name = name;
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            String mbean_object_name = MBEAN_NAME_PREFIX + name;
            mbs.registerMBean(this, new ObjectName(mbean_object_name));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public DefaultListener() {
        this("unknown");
    }

    public String getCurrentSummaryXml() {
        try {
            readLock.lock();
            checkRecord();
            return xmlHeader + curRecord.getSummaryXml();
        } finally {
            readLock.unlock();
        }
    }

    public String getCurrentDetailXml() {
        try {
            readLock.lock();
            checkRecord();
            return xmlHeader + curRecord.getDetailXml();
        } finally {
            readLock.unlock();
        }
    }

    public String getGlobalSummaryXml() {
        try {
            readLock.lock();
            return xmlHeader + avgRecord.getSummaryXml();
        } finally {
            readLock.unlock();
        }
    }

    public String getGlobalDetailXml() {
        try {
            readLock.lock();
            return xmlHeader + avgRecord.getDetailXml();
        } finally {
            readLock.unlock();
        }
    }

    private static String historyKeyWord = "history";
    private static String historyPrefix = "<" + historyKeyWord + ">";
    private static String historysuffix = "</" + historyKeyWord + ">";

    public String getHistorySummaryXml() {
        try {
            StringBuilder sb = new StringBuilder();
            readLock.lock();
            sb.append(xmlHeader);
            sb.append(historyPrefix);
            for (PerformanceRecord r : records) {
                sb.append(r.getSummaryXml());
            }
            sb.append(historysuffix);
            return sb.toString();
        } finally {
            readLock.unlock();
        }
    }

    public String getHistoryDetailXml() {
        try {
            StringBuilder sb = new StringBuilder();
            readLock.lock();
            sb.append(xmlHeader);
            sb.append(historyPrefix);
            for (PerformanceRecord r : records) {
                sb.append(r.getDetailXml());
            }
            sb.append(historysuffix);
            return sb.toString();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String url(boolean detail) {
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    private void genPerfDesc() {
        if (curRecord != null) {
            StringBuffer sb = new StringBuffer();
            // 1309178640000 230 25 7384 68258 230 25 7384 3.30559507897
            // 3.30559507897
            /*
             * return "%s %s %s %s %s %s %s %s %s %s"%( self.bucket,
             * self.objNum, self.minDelay, self.maxDelay, self.reDelay,
             * self.totalNum, self.totalMinDelay, self.totalMaxDelay,
             * self.bucketAvg, self.totalAvg)
             */
            sb.append(String.format("%s %d %d %d %d %d %d %d %d %s",
                    DateUtil.long2Str(curRecord.getEndTs()),
                    curRecord.getSuccNum(), curRecord.getMinDelay(),
                    curRecord.getMaxDelay(), curRecord.getTotalDelay(),
                    avgRecord.getSuccNum(), avgRecord.getMinDelay(),
                    avgRecord.getMaxDelay(), curRecord.getAvgDelay(),
                    avgRecord.getAvgDelay()));

            historys.addLast(sb.toString());
            if (historys.size() > historyNum) {
                historys.removeFirst();
            }
        }
    }

    private void checkRecord() {
        long ts = tsPoint();
        if (curRecord == null || !curRecord.hit(ts)) {
            genPerfDesc();

            curRecord = new PerformanceRecord(ts, ts + interval, tsUnit);
            records.addFirst(curRecord);
            recordMap.put(curRecord.getStartTs(), curRecord);
            if (records.size() > historyNum) {
                oldRecord = records.removeLast();
                if (oldRecord != null) {
                    recordMap.remove(oldRecord.getStartTs());
                }
            }
        }
    }

    @Override
    public void deliver() {
        try {
            writeLock.lock();
            checkRecord();
            avgRecord.deliver();
            curRecord.deliver();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void fail() {
        try {
            writeLock.lock();
            checkRecord();
            avgRecord.fail();
            curRecord.fail();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void succ(long delay) {
        try {
            writeLock.lock();
            checkRecord();
            avgRecord.succ(delay);
            curRecord.succ(delay);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String global(String lineSep, boolean detail) {
        try {
            readLock.lock();
            StringBuilder sb = new StringBuilder();
            sb.append(String
                    .format("-------- begin of %s global performance statistics -----%s",
                            name, lineSep));
            synchronized (lock) {
                sb.append(avgRecord.toString(lineSep, detail));
            }
            sb.append(String
                    .format("-------- end of %s global performance statistics -----%s%s",
                            name, lineSep, lineSep));
            return sb.toString();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String globalAndHistory(String lineSep, boolean detail) {
        try {
            readLock.lock();
            StringBuilder sb = new StringBuilder();
            sb.append(String.format(
                    "-------- begin of %s all performance statistics -----%s",
                    name, lineSep));
            synchronized (lock) {
                sb.append(String
                        .format("-------- begin of %s global performance statistics -----%s",
                                name, lineSep));
                sb.append(avgRecord.toString(lineSep, detail));
                sb.append(String
                        .format("-------- end of %s global performance statistics -----%s%s",
                                name, lineSep, lineSep));
                sb.append(String
                        .format("-------- begin of %s history performance statistics -----%s",
                                name, lineSep));
                for (PerformanceRecord r : records) {
                    sb.append(r.toString(lineSep, detail));
                }
                sb.append(String
                        .format("-------- end of %s history performance statistics -----%s%s",
                                name, lineSep, lineSep));
            }
            sb.append(String.format(
                    "-------- end of %s all performance statistics -----%s%s",
                    name, lineSep, lineSep));
            return sb.toString();
        } finally {
            readLock.unlock();
        }
    }

    public String history(String lineSep, boolean detail) {
        try {
            readLock.lock();
            StringBuilder sb = new StringBuilder();
            sb.append(String
                    .format("-------- begin of %s history performance statistics -----%s",
                            name, lineSep));
            synchronized (lock) {
                for (PerformanceRecord r : records) {
                    sb.append(r.toString(lineSep, detail));
                }
            }
            sb.append(String
                    .format("-------- end of %s history performance statistics -----%s%s",
                            name, lineSep, lineSep));
            return sb.toString();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String toString() {
        return toString(Constant.LINE_SEP, true);
    }

    public String toString(String lineSep, boolean detail) {
        return globalAndHistory(lineSep, detail);
    }

    public static void flushFile(String name, String content) {
        try {
            FileOutputStream fout = new FileOutputStream(new File(
                    "D:\\study\\java\\pf\\" + name + ".xml"));
            fout.write(content.getBytes());
            fout.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] argv) {
        DefaultListener rec = new DefaultListener(); // 60s
        long startTs = System.currentTimeMillis();
        Random r = new Random(startTs);
        long max = 10;
        long delay = 0;
        long stopTs = startTs + 120000L;
        long ts = 0;

        do {
            rec.deliver();
            delay = Math.abs((long) ((r.nextGaussian() * max)) % max) + 1L;
            if (r.nextBoolean()) {
                rec.succ(delay);
            } else {
                rec.fail();
            }

            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ts = System.currentTimeMillis();
            System.out.println("left " + (stopTs - ts) + " ms");
        } while (ts < stopTs);

        System.out.println(rec.getCurrentSummaryXml());
        flushFile("curDetail", rec.getCurrentDetailXml());
        flushFile("curSummary", rec.getCurrentSummaryXml());
        flushFile("globalDetail", rec.getGlobalDetailXml());
        flushFile("globalSummary", rec.getGlobalSummaryXml());
        flushFile("historyDetail", rec.getHistoryDetailXml());
        flushFile("historySummary", rec.getHistorySummaryXml());
    }

    @Override
    public long getCurrent90Delay() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getSpecDelay(90.0f);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getCurrentAvgDelay() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getAvgDelay();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getCurrentMaxDelay() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getMaxDelay();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getCurrentMinDelay() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getMinDelay();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getCurrentThroughput() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getThroughput();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String getTotalStartTimestamp() {
        try {
            readLock.lock();
            return avgRecord.getStartTsStr();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String getTotalStopTimestamp() {
        try {
            readLock.lock();
            return avgRecord.getEndTsStr();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getTotal90Delay() {
        try {
            readLock.lock();
            return avgRecord.getSpecDelay(90.0f);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getTotalAvgDelay() {
        try {
            readLock.lock();
            return avgRecord.getAvgDelay();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getTotalMaxDelay() {
        try {
            readLock.lock();
            return avgRecord.getMaxDelay();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getTotalMinDelay() {
        try {
            readLock.lock();
            return avgRecord.getMinDelay();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getTotalThroughput() {
        try {
            readLock.lock();
            return avgRecord.getThroughput();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String getCurrentStartTimestamp() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getStartTsStr();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String getCurrentStopTimestamp() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getEndTsStr();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getCurrentSuccNum() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getSuccNum();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getCurrentFailNum() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getFailNum();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public float getCurrentSuccRate() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getSuccRate();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public float getCurrentFailRate() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getFailRate();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getCurrentFinishNum() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getFinishNum();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getCurrentPendNum() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getPendNum();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getCurrentDeliverNum() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getDeliverNum();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public float getCurrentPendRate() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getPendRate();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public float getCurrentFinishRate() {
        try {
            readLock.lock();
            checkRecord();
            return curRecord.getFinishRate();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getTotalSuccNum() {
        try {
            readLock.lock();
            return avgRecord.getSuccNum();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getTotalFailNum() {
        try {
            readLock.lock();
            return avgRecord.getFailNum();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public float getTotalSuccRate() {
        try {
            readLock.lock();
            return avgRecord.getSuccRate();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public float getTotalFailRate() {
        try {
            readLock.lock();
            return avgRecord.getFailRate();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getTotalFinishNum() {
        try {
            readLock.lock();
            return avgRecord.getFinishNum();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getTotalPendNum() {
        try {
            readLock.lock();
            return avgRecord.getPendNum();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getTotalDeliverNum() {
        try {
            readLock.lock();
            return avgRecord.getDeliverNum();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public float getTotalPendRate() {
        try {
            readLock.lock();
            return avgRecord.getPendRate();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public float getTotalFinishRate() {
        try {
            readLock.lock();
            return avgRecord.getFinishRate();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String getCurrentStatus() {
        StringBuilder sb = new StringBuilder();
        try {
            readLock.lock();
            if (historys.isEmpty()) {
                return null;
            } else {
                for (String s : historys) {
                    sb.append(s);
                    sb.append(Constant.LINE_SEP);
                }
                historys.clear();
            }
        } finally {
            readLock.unlock();
        }
        return sb.toString();
    }

    @Override
    public String getDelayDistribute() {
        try {
            readLock.lock();
            return this.avgRecord.toString();
        } finally {
            readLock.unlock();
        }

    }
}
