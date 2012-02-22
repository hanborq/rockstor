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

package com.rockstor.core.io;

//import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;

import com.rockstor.util.MultiCache;
import com.rockstor.util.RockConfiguration;

public class RockReaderPool implements RockAccessorPool<RockReader> {
    private static RockReaderPool instance;
    private static final String ROCK_READER_CACHE_SIZE = "rock.reader.cache.num";
    private MultiCache rockCache;
    private static final String MBEAN_SUBFIX = "RockReadPool";
    private Configuration conf = RockConfiguration.getDefault();

    public static synchronized RockReaderPool getInstance() {
        if (instance == null)
            instance = new RockReaderPool();
        return instance;
    }

    private RockReaderPool() {
        init();
    }

    @Override
    public void close() {
        rockCache.clean();
    }

    @Override
    public RockReader get(String rockID) {
        return rockCache.apply(rockID);
    }

    @Override
    public void init() {
        int cacheSize = conf.getInt(ROCK_READER_CACHE_SIZE, 1024);
        if (cacheSize < 1024) {
            cacheSize = 1024;
        }
        rockCache = new MultiCache(cacheSize, MBEAN_SUBFIX);

    }

    @Override
    public void release(RockReader accessor) {
        rockCache.release(accessor);
    }

    @Override
    public void remove(RockReader accessor) {
        rockCache.remove(accessor);
    }

    @Override
    public int size() {
        return rockCache.getSize();
    }

    @Override
    public RockReader get() {
        throw new UnsupportedOperationException();
    }
}
