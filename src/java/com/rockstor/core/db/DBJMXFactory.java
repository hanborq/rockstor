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

package com.rockstor.core.db;

import java.util.concurrent.ConcurrentHashMap;

public final class DBJMXFactory {
    private static DBJMXFactory instance = null;
    private ConcurrentHashMap<String, DBJMX> dbJmxs = new ConcurrentHashMap<String, DBJMX>();

    private DBJMXFactory() {
    }

    public static DBJMXFactory getInstance() {
        if (null == instance) {
            instance = new DBJMXFactory();
        }
        return instance;
    }

    public DBJMX getDBJMX(String tabName) {
        DBJMX ret = dbJmxs.get(tabName);
        if (ret == null) {
            ret = new DBJMX(tabName);
            dbJmxs.put(tabName, ret);
        }
        return ret;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
