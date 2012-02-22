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
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;

public class LoggerServer implements Closeable {
    private File rootDir = null;

    private Map<String, Timer> timers = new HashMap<String, Timer>();
    private Map<String, RemoteServer> servs = new HashMap<String, RemoteServer>();
    private List<Logger> logs = new LinkedList<Logger>();
    private static String timerNamePrefix = "performanceLogServer_";
    private JoinThread joinThread = new JoinThread();

    private static class JoinThread extends Thread {
        public JoinThread() {
            super();
        }

        private boolean run = true;

        @Override
        public void run() {
            try {
                while (run) {
                    sleep(10L);
                }
            } catch (InterruptedException e) {

            }
        }

        public void exit() {
            run = false;
        }
    }

    public LoggerServer() {
        super();
    }

    public String getRootDir() {
        assert (rootDir != null);
        return rootDir.getAbsolutePath();
    }

    public void setRootDir(String rootDir) {
        assert (rootDir != null);
        this.rootDir = new File(rootDir);
        assert ((!this.rootDir.exists()) || this.rootDir.isDirectory());
    }

    public void setServers(RemoteServer[] servs) {
        this.servs.clear();
        if (servs != null && servs.length > 0) {
            for (RemoteServer serv : servs) {
                this.servs.put(serv.getAddress(), serv);
            }
        }

    }

    public void addServer(RemoteServer serv) {
        assert (serv != null);
        servs.put(serv.getAddress(), serv);
    }

    public void start() throws IOException {
        if (!rootDir.exists()) {
            rootDir.mkdirs();
        }

        RemoteServer serv = null;
        List<Item> items = null;
        for (Map.Entry<String, RemoteServer> entry : servs.entrySet()) {
            serv = entry.getValue();
            items = serv.getItems();
            for (Item item : items) {
                logs.add(new Logger(rootDir, serv, item));
            }
            timers.put(entry.getKey(),
                    new Timer(timerNamePrefix + entry.getKey(), true));
        }

        for (Logger log : logs) {
            log.start(timers.get(log.getServer().getAddress()));
        }

        joinThread.start();
    }

    public void join() {
        try {
            joinThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        for (Logger log : logs) {
            log.close();
        }

        logs.clear();

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            entry.getValue().cancel();
        }

        timers.clear();

        joinThread.exit();
    }

}
