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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class Host {

    private static Logger LOG = Logger.getLogger(Host.class);
    private static Configuration conf = RockConfiguration.getDefault();
    private static String hostName = null;

    static {
        hostName = conf.get("rock.host.name", null);
        if (hostName == null) {
            try {
                hostName = InetAddress.getLocalHost().getHostAddress() + "_"
                        + InetAddress.getLocalHost().getHostName();
                LOG.info("Load host name from NIC,  host name = " + hostName);
            } catch (UnknownHostException e) {
                LOG.error("Get host name catch UnknownHostException, exit : "
                        + ExceptionLogger.getStack(e));
                System.exit(-1);
            }
        } else {
            LOG.info("Load host name from RockConf,  host name = " + hostName);
        }
    }

    public static String getHostName() {
        return hostName;
    }
}
