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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.rockstor.monitor.Listener;
import com.rockstor.monitor.ListenerFactory;
import com.rockstor.task.Task;
import com.rockstor.util.RockConfiguration;

public class BaseThread extends Thread {
    private static Logger LOG = Logger.getLogger(BaseThread.class);
    private ConcurrentLinkedQueue<Task> queue;
    private static final int DEFAULT_QUEUE_SIZE = 10;
    private AtomicBoolean running = new AtomicBoolean(true);
    private Listener listener = null;
    private static Configuration conf = RockConfiguration.getDefault();
    private static long threadSleepInterval = 1L;
    static {
        threadSleepInterval = conf.getLong("rockstor.thread.sleepInterval",
                threadSleepInterval);
        if (threadSleepInterval < 0) {
            threadSleepInterval = 1;
        }
    }

    public BaseThread(int queueSize, String name) {
        if (name != null && name.length() > 0) {
            setName(name);
            listener = ListenerFactory.getInstance().getListener(name);
        }
        if (queueSize < DEFAULT_QUEUE_SIZE) {
            queueSize = DEFAULT_QUEUE_SIZE;
        }
        queue = new ConcurrentLinkedQueue<Task>();
        // this.setPriority(Thread.MAX_PRIORITY);
    }

    public BaseThread() {
        this(DEFAULT_QUEUE_SIZE, null);
    }

    public BaseThread(String name) {
        this(DEFAULT_QUEUE_SIZE, name);
    }

    public BaseThread(int queueSize) {
        this(queueSize, null);
    }

    public void addTask(Task task) {
        listener.deliver();
        queue.add(task);
    }

    public void removeTask(Task task) {
        queue.remove(task);
    }

    public void end() {
        // Task.resetTaskNum();
        while (Task.getLeftTaskNum() > 0) {
            try {
                System.out
                        .println("stopping " + getName() + ", queue size: "
                                + queue.size() + ", left task "
                                + Task.getLeftTaskNum());
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        running.set(false);
        interrupt();
        System.out.println(getName() + " stopped ok!");
    }

    @Override
    public void run() {
        Task task = null;
        int queueSize = 0;
        long ts = 0;
        while (running.get() || Task.getLeftTaskNum() > 0) {
            try {
                queueSize = queue.size();

                for (int i = 0; i < queueSize; i++) {
                    task = queue.poll();
                    ts = System.currentTimeMillis();
                    if (task != null) {
                        LOG.info(getName() + " schedule " + task);
                        task.exec();
                        listener.succ(System.currentTimeMillis() - ts);
                    } else {
                        // System.out.println("Thread_"+getName()+" got null task");
                        break;
                    }
                }

                Thread.sleep(threadSleepInterval);
            } catch (InterruptedException e) {
                // System.out.println("Thread #"+getName()+" interrupted!");
                break;
            }
        }
        LOG.info(getName() + " stopped!");
    }

    /**
     * @param args
     */
    public static void main(String[] args) {

    }

}
