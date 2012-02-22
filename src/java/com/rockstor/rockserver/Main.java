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

package com.rockstor.rockserver;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.xml.XmlConfiguration;
import org.xml.sax.SAXException;

import com.rockstor.util.RockConfiguration;

public class Main {
    /**
     * @param args
     */
    public static void main(String[] args) {
        Configuration rockConf = RockConfiguration.getDefault();
        int listenPort = 8080;
        listenPort = rockConf.getInt("rockstor.rockserver.listenPort",
                listenPort);

        String hostname = "0.0.0.0";
        hostname = rockConf.get("rock.host.name", hostname);

        System.setProperty("jetty.host", hostname);
        System.setProperty("jetty.port", String.valueOf(listenPort));
        try {
            XmlConfiguration jettyConf = new XmlConfiguration(
                    new FileInputStream(Main.class.getClassLoader()
                            .getResource("jetty-rockserver.xml").getPath()));

            Server server = new Server();
            jettyConf.configure(server);

            server.start();
            System.out.println("Monitor Server listening on : " + listenPort);
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
