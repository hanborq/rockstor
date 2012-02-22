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

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class RockConfiguration extends Configuration {
    private static Logger LOG = Logger.getLogger(RockConfiguration.class);
    private static final Configuration defaultConf = create();

    private static void checkDefaultsVersion(Configuration conf) {
        String defaultsVersion = conf.get("rockstor.defaults.for.version");
        String thisVersion = VersionInfo.getVersion();
        if (!thisVersion.equals(defaultsVersion)) {
            throw new RuntimeException(
                    "rockstor-default.xml file seems to be for and old version of RockStor ("
                            + defaultsVersion + "), this version is "
                            + thisVersion + ", version: " + VersionInfo.info());
        }
    }

    public static Configuration addRockStorResources(Configuration conf) {
        conf.addResource("rockstor-default.xml");
        conf.addResource("rockstor-site.xml");
        LOG.debug("rockstor.bin.home: " + conf.getResource("./"));
        LOG.debug("rockstor.default.config.path: "
                + conf.getResource("rockstor-default.xml"));
        LOG.debug("rockstor.user.config.path: "
                + conf.getResource("rockstor-site.xml"));
        checkDefaultsVersion(conf);
        return conf;
    }

    public static String str(Configuration conf) {
        StringBuilder sb = new StringBuilder();
        Iterator<Entry<String, String>> it = conf.iterator();
        Entry<String, String> entry = null;
        sb.append("Rockstor configuration\r\n");
        while (it.hasNext()) {
            entry = it.next();
            sb.append(String.format("%32s: %s\r\n", entry.getKey(),
                    entry.getValue()));
        }
        sb.append("---------------\r\n");
        return sb.toString();
    }

    public static final Configuration getDefault() {
        return defaultConf;
    }

    /**
     * Creates a Configuration with RockStor resources
     * 
     * @return a Configuration with RockStor resources
     */
    public static Configuration create() {
        Configuration conf = new Configuration();
        return addRockStorResources(conf);
    }

    /**
     * Creates a clone of passed configuration.
     * 
     * @param that
     *            Configuration to clone.
     * @return a Configuration created with the rockstor-*.xml files plus the
     *         given configuration.
     */
    public static Configuration create(final Configuration that) {
        Configuration conf = getDefault();
        for (Entry<String, String> e : that) {
            conf.set(e.getKey(), e.getValue());
        }
        return conf;
    }
}
