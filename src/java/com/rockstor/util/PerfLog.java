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

package com.rockstor.util;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

public class PerfLog implements Closeable {
    private static class Record {
        private int FLUSH_NUM = 400;
        private long STEP = 10; // 10ms
        private int seq = 0;
        private TreeMap<String, TreeMap<Long, Long>> s = new TreeMap<String, TreeMap<Long, Long>>();

        public Record(int rollNum, long step) {
            FLUSH_NUM = rollNum;
            STEP = step;
        }

        public boolean rollable() {
            return seq >= FLUSH_NUM;
        }

        public void put(String opType, long delay) {
            delay = 1 + (delay / STEP);
            TreeMap<Long, Long> curMap = null;
            if (!s.containsKey(opType)) {
                curMap = new TreeMap<Long, Long>();
                s.put(opType, curMap);
            } else {
                curMap = s.get(opType);
            }

            if (curMap.containsKey(delay)) {
                curMap.put(delay, curMap.get(delay) + 1);
            } else {
                curMap.put(delay, 1L);
            }
            ++seq;
        }

        public void flush(DataOutputStream fos) {
            try {
                for (Entry<String, TreeMap<Long, Long>> item1 : s.entrySet()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(item1.getKey() + ":\r\n");
                    for (Entry<Long, Long> item2 : item1.getValue().entrySet()) {
                        sb.append("\t" + item2.getKey() + ": "
                                + item2.getValue());
                        sb.append("\r\n");
                    }
                    fos.writeBytes(sb.toString());
                }

            } catch (IOException e) {
                // e.printStackTrace();
            } finally {

                try {
                    fos.writeBytes("-----------\r\n");
                    fos.flush();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    // e.printStackTrace();
                }

                seq = 0;
                s.clear();
            }
        }
    }

    private DataOutputStream fos = null;
    private int seq = 0;
    private File prefix = null;
    private String prefix_str = null;
    private Record curRecord = null;

    private int rollInterval = 100;
    private int fileSeq = 0;

    public PerfLog(String prefix_name, int recordRoll, int fileRoll,
            int delayStep) {
        this.prefix = new File(prefix_name);
        prefix_str = prefix.getName();
        File par = new File(prefix.getParent());
        format(par);

        if (fileRoll < 5) {
            fileRoll = 5;
        }
        if (recordRoll < 100) {
            recordRoll = 100;
        }

        if (delayStep < 10) {
            delayStep = 10;
        }
        curRecord = new Record(recordRoll, delayStep);
        rollInterval = fileRoll;
        roll();
    }

    public PerfLog(String prefix_name) {
        this(prefix_name, 0, 0, 0);
    }

    public PerfLog(String prefix_name, int recordRoll, int fileRoll) {
        this(prefix_name, recordRoll, fileRoll, 0);
    }

    private void format(File par) {
        if (par.exists()) {
            File[] files = par.listFiles();
            for (File f : files) {
                if (f.getName().startsWith(prefix_str)) {
                    f.delete();
                }
            }
        } else {
            par.mkdirs();
        }
    }

    public synchronized void put(String opType, long delay) {
        curRecord.put(opType, delay);
        if (curRecord.rollable()) {
            curRecord.flush(fos);
            ++seq;
            if (seq % rollInterval == 0) {
                roll();
            }
        }
    }

    private void roll() {
        File outFile = new File(prefix.getAbsolutePath() + "." + (fileSeq++));
        try {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            fos = new DataOutputStream(new FileOutputStream(outFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        if (fos != null) {
            curRecord.flush(fos);
            fos.close();
        }

    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        PerfLog pl = new PerfLog("./perlog/get", 400, 100);
        String[] op = new String[] { "put", "get", "delete", "remove" };
        int opNum = op.length;
        Random r = new Random();
        for (int i = 0; i < 10000; i++) {
            pl.put(op[i % opNum], (Math.abs(r.nextLong()) % 100) + 10);
        }

        try {
            pl.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
