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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.rockstor.core.db.RockDB;
import com.rockstor.core.io.RockReader;
import com.rockstor.core.meta.Rock;

/**
 * @author terry a lru resource cache which can persist several values for the
 *         same key. in this version, we use a interface @CacheValueGenerator to
 *         generator value for the specified key.
 * @param <V>
 */

public class MultiCache {
    private static Logger LOG = Logger.getLogger(MultiCache.class);

    private int maxSize = 1024;

    TreeMap<Long, RockReader> free = new TreeMap<Long, RockReader>();
    TreeMap<Integer, RockReader> busy = new TreeMap<Integer, RockReader>();
    HashMultimap<String, RockReader> freeMap = HashMultimap.create();

    private Object lockObject = new Object();

    public RockReader getRockReader(String key) {
        Rock rock = null;
        byte[] rockID = MD5HashUtil.bytesFromHexString(key);
        try {
            rock = RockDB.getMeta(rockID);
        } catch (Exception e) {
            ExceptionLogger.log(LOG, e);
        } finally {
            if (rock == null) {
                return null;
            }
        }

        RockReader rockReader = null;
        try {
            rockReader = new RockReader(rockID);
            rockReader.setCtime(rock.getCtime());
            rockReader.setRockVersion(rock.getRockVersion());

            rockReader.open();
        } catch (IOException e) {
            ExceptionLogger.log(LOG, e);
            try {
                rockReader.close();
            } catch (IOException e1) {
                ExceptionLogger.log(LOG, e1);
            }
            rockReader = null;
        }

        return rockReader;
    }

    private static final String MBEAN_OBJECT_NAME_PREFIX = "com.rockstor.util:type=";
    private MultiCacheStatus cs = null;

    public MultiCache(int capacity, String poolName) {
        maxSize = capacity;
        cs = new MultiCacheStatus(this);
        String mbean_object_name = MBEAN_OBJECT_NAME_PREFIX + poolName;

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        LOG.info("Register MultiCacheStatusMBean: " + mbean_object_name);
        try {
            mbs.registerMBean(cs, new ObjectName(mbean_object_name));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int getSize() {
        synchronized (lockObject) {
            return free.size() + busy.size();
        }
    }

    public int getMaxNum() {
        return maxSize;
    }

    public int getBusyNum() {
        synchronized (lockObject) {
            return busy.size();
        }
    }

    /**
     * apply a resource from cache
     * 
     * @param key
     *            resource key
     * @return resource if cache is not full with busy resources. null, create
     *         new resource failed, or cache is full, and all resources are
     *         busy.
     */
    public RockReader apply(String key) {
        cs.request();
        RockReader item = null;
        synchronized (lockObject) {
            if (freeMap.containsKey(key)) {
                Set<RockReader> items = freeMap.get(key);
                item = items.toArray(new RockReader[0])[0];
                freeMap.remove(key, item);
                free.remove(item.getTs());

                busy.put(item.getCrID(), item);

                item.hit();
                cs.hit();

                LOG.info("Apply " + key + " OK, " + item + ", [busy :"
                        + busy.size() + ", free: " + free.size()
                        + ", freeMap: " + freeMap.size() + "]" + cs);
            } else {
                cs.lost();
                if (busy.size() > maxSize) {
                    cs.overload();
                    LOG.warn("Apply " + key
                            + " Failed, System is Busy, [busy :" + busy.size()
                            + ", free: " + free.size() + ", freeMap: "
                            + freeMap.size() + "]" + cs);
                    return item;
                }

                // drop one
                if (free.size() + busy.size() > maxSize) {
                    RockReader dropped = free.firstEntry().getValue();
                    free.remove(dropped.getTs());
                    freeMap.remove(dropped.getRockID(), dropped);
                    cs.drop();
                    LOG.info("DROP " + dropped + ", [busy :" + busy.size()
                            + ", free: " + free.size() + ", freeMap: "
                            + freeMap.size() + "]" + cs);
                    try {
                        dropped.close();
                    } catch (IOException e) {

                    }

                }

                item = getRockReader(key);

                if (item == null) {
                    LOG.error("Apply " + key
                            + " Failed, create RockReader Failed, [busy :"
                            + busy.size() + ", free: " + free.size()
                            + ", freeMap: " + freeMap.size() + "]" + cs);
                    return null;
                }

                busy.put(item.getCrID(), item);
                LOG.info("Apply " + key + " OK, " + item + ", [busy :"
                        + busy.size() + ", free: " + free.size()
                        + ", freeMap: " + freeMap.size() + "]" + cs);
            }
        }
        return item;
    }

    /**
     * release resource to the cache
     * 
     * @param key
     * @param v
     * @return true if release successful, else false
     */
    public void release(RockReader item) {
        synchronized (lockObject) {
            if (busy.containsKey(item.getCrID())) {
                busy.remove(item.getCrID());
                free.put(item.getTs(), item);
                freeMap.put(item.getRockID(), item);
                LOG.info("Release " + item + " OK, [busy :" + busy.size()
                        + ", free: " + free.size() + ", freeMap: "
                        + freeMap.size() + "]" + cs);
            } else {
                LOG.warn("Release " + item
                        + " Failed, not in busy map, [busy :" + busy.size()
                        + ", free: " + free.size() + ", freeMap: "
                        + freeMap.size() + "]" + cs);
            }
        }
    }

    /**
     * remove resouce from cache when something wrong happened with the resource
     * 
     * @param key
     * @param v
     * @return
     */
    public void remove(RockReader item) {
        assert (item != null);
        cs.remove();
        synchronized (lockObject) {
            if (busy.containsKey(item.getCrID())) {
                busy.remove(item.getCrID());
                try {
                    item.close();
                } catch (IOException e) {
                    ExceptionLogger.log(LOG, e);
                }

                LOG.info("Remove " + item + " OK, [busy :" + busy.size()
                        + ", free: " + free.size() + ", freeMap: "
                        + freeMap.size() + "]" + cs);
            } else {
                LOG.warn("Remove " + item + " Failed, not in busy map, [busy :"
                        + busy.size() + ", free: " + free.size()
                        + ", freeMap: " + freeMap.size() + "]" + cs);
            }
        }
    }

    /**
     * remove all resources from cache, wait all of the resources are released
     * to the cache, then close all resource
     */
    public void clean() {
        synchronized (lockObject) {
            // wait until all of the resources are released to the cache.

            for (Entry<Long, RockReader> entry : free.entrySet()) {
                try {
                    entry.getValue().close();
                } catch (IOException e1) {

                }
            }

            // wait until all of the resources are released to the cache.
            for (Entry<Integer, RockReader> entry : busy.entrySet()) {
                try {
                    entry.getValue().close();
                } catch (IOException e1) {

                }
            }

            busy.clear();
            freeMap.clear();
            free.clear();

            LOG.info("clean OK!, final cache status: " + cs);
        }
    }

    @Override
    public String toString() {
        synchronized (lockObject) {
            StringBuffer sb = new StringBuffer();
            sb.append("-------Free---------\n");
            for (Entry<Long, RockReader> entry : free.entrySet()) {
                sb.append(entry.getValue() + "\n");
            }
            sb.append("-------Busy---------\n");

            for (Entry<Integer, RockReader> entry : busy.entrySet()) {
                sb.append(entry.getValue() + "\n");
            }

            sb.append("\n");
            sb.append("[busy :" + busy.size() + ", free: " + free.size()
                    + ", freeMap: " + freeMap.size() + "]");

            return sb.toString();
        }
    }

    public static void testHashMultiMap() {
        HashMultimap<String, Integer> m = HashMultimap.create();
        m.put("1", 1);
        m.put("1", 10);
        m.put("2", 2);

        System.out.println(m.size());
        m.remove("2", 2);
        System.out.println(m.size());
        Set<Integer> set = m.get("2");
        System.out.println(set);
    }

    public long getDropRate() {
        return cs.getDropRate();
    }

    public long getFailedRate() {
        return cs.getFailedRate();
    }

    public long getHitRate() {
        return cs.getHitRate();
    }

    public long getLostRate() {
        return cs.getLostRate();
    }

    public long getOverloadRate() {
        return cs.getOverloadRate();
    }
}
