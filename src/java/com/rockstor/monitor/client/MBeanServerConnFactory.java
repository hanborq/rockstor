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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class MBeanServerConnFactory {
    private static String connStrFmt = "service:jmx:rmi:///jndi/rmi://%s/jmxrmi";
    private static Map<String, JMXConnector> connMap = new HashMap<String, JMXConnector>();
    private static ReentrantLock lock = new ReentrantLock();

    public static MBeanServerConnection getMBeanServerConn(String address) {
        MBeanServerConnection servConn = null;
        JMXConnector conn = null;
        try {
            lock.lock();
            conn = connMap.get(address);
            if (conn != null) {
                servConn = conn.getMBeanServerConnection();
            } else {
                JMXServiceURL jmxServURL = new JMXServiceURL(String.format(
                        connStrFmt, address));
                conn = JMXConnectorFactory.connect(jmxServURL);
                if (conn != null) {
                    connMap.put(address, conn);
                    servConn = conn.getMBeanServerConnection();
                }
            }
        } catch (Exception e) {

        } finally {
            lock.unlock();
        }
        return servConn;
    }

    public static void dropMBeanServerConn(String address) {
        JMXConnector conn = null;
        try {
            lock.lock();
            conn = connMap.get(address);
            if (conn != null) {
                connMap.remove(address);
                conn.close();
            }
        } catch (Exception e) {

        } finally {
            lock.unlock();
        }
    }

    public static void clean() {
        JMXConnector conn = null;
        try {
            lock.lock();
            for (Map.Entry<String, JMXConnector> entry : connMap.entrySet()) {
                conn = entry.getValue();
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (Exception e) {

                    }
                }
            }

            connMap.clear();
        } catch (Exception e) {

        } finally {
            lock.unlock();
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {

    }

}
