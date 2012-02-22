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

import java.io.FileInputStream;
import java.io.IOException;

import org.eclipse.jetty.xml.XmlConfiguration;
import org.xml.sax.SAXException;

import com.rockstor.rockserver.Main;

public class MonitorClient {

    public static void main(String[] args) {
        try {
            String p = Main.class.getClassLoader()
                    .getResource("perf-rockserver.xml").getPath();
            System.out.println(p);
            XmlConfiguration jettyConf = new XmlConfiguration(
                    new FileInputStream(p));

            LoggerServer server = new LoggerServer();
            jettyConf.configure(server);
            server.start();
            server.join();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }
}
