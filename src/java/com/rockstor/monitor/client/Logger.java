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

package com.rockstor.monitor.client;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ReflectionException;

import com.rockstor.Constant;
import com.rockstor.util.DateUtil;

public class Logger extends TimerTask implements Closeable {
    private Item item = null;;
    private RemoteServer serv = null;
    private File rootDir = null;
    private File outFile = null;
    private FileWriter fout = null;
    private static final long ONE_DAY = 24 * 3600 * 1000L;
    private long nextFetchTime = 0;
    private long nextRollTime = 0;
    private boolean initialized = false;
    private MBeanServerConnection mbServConn = null;

    public Logger() {
        this(null, null, null);
    }

    public Logger(File rootDir, RemoteServer serv, Item item) {
        this.rootDir = rootDir;
        this.serv = serv;
        this.item = item;
    }

    private void init() throws IOException {
        if (initialized) {
            return;
        }

        long curTs = System.currentTimeMillis();
        nextFetchTime = curTs + item.getInterval()
                - (curTs % (item.getInterval())) - (item.getInterval() / 2);
        nextRollTime = curTs - (curTs % ONE_DAY);
        initialized = true;
        if (mbServConn == null) {
            mbServConn = MBeanServerConnFactory.getMBeanServerConn(serv
                    .getAddress());
        }
        roll();
        initialized = true;
    }

    // log file format: rootDir/YYYYMMDD/objName/host_attr.txt
    private static String filePathFormat = "%s/%s/%s/%s_%s.txt"; // rootDir/YYYYMMDD/objName/attr_host.txt

    private void roll() throws IOException {
        if (nextRollTime > nextFetchTime) {
            return;
        }

        closeFile();

        String fname = String.format(filePathFormat,
                rootDir.getCanonicalPath(), DateUtil.long2Str(nextRollTime)
                        .substring(0, 10), item.getSuffix(),
                item.getAttrName(), serv.getHost());

        outFile = new File(fname);

        File parFile = outFile.getParentFile();

        if (!parFile.exists()) {
            parFile.mkdirs();
        }

        fout = new FileWriter(outFile, outFile.exists());

        nextRollTime += ONE_DAY;
    }

    private void closeFile() throws IOException {
        if (fout != null) {
            fout.close();
            fout = null;
        }
    }

    @Override
    public void close() throws IOException {
        closeFile();
        cancel();
    }

    @Override
    public void run() {
        try {
            if (mbServConn == null) {
                mbServConn = MBeanServerConnFactory.getMBeanServerConn(serv
                        .getAddress());
                if (mbServConn == null) {
                    System.out.println(serv + " " + item
                            + " Got mb serv failed!");
                    return;
                }
            }

            roll();
            String curValue = null;
            Object value = mbServConn.getAttribute(item.getObject(),
                    item.getAttrName());
            if (value != null) {
                curValue = value.toString();
                fout.write(curValue);
                if (!curValue.endsWith("\n")) {
                    fout.write(Constant.LINE_SEP);
                }
                fout.flush();

                curValue = String.format("%s %s",
                        DateUtil.long2Str(nextFetchTime), curValue);
                System.out.println(serv + " " + item + " Got " + curValue);
            }
        } catch (IOException e) {

        } catch (AttributeNotFoundException e) {
            // e.printStackTrace();
        } catch (InstanceNotFoundException e) {
            // e.printStackTrace();
        } catch (MBeanException e) {
            // e.printStackTrace();
        } catch (ReflectionException e) {
            // e.printStackTrace();
        } finally {
            nextFetchTime += item.getInterval();
        }
    }

    public void start(Timer timer) throws IOException {
        init();
        Date firstDate = new Date();
        firstDate.setTime(nextFetchTime);
        timer.schedule(this, firstDate, item.getInterval());
    }

    public Item getItem() {
        return item;
    }

    public void setItem(Item item) {
        this.item = item;
    }

    public RemoteServer getServer() {
        return serv;
    }

    public void setServer(RemoteServer serv) {
        this.serv = serv;
    }

    public File getRootDir() {
        return rootDir;
    }

    public void setRootDir(File rootDir) {
        this.rootDir = rootDir;
    }
}
