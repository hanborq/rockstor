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

package com.rockstor.core.meta;

import java.util.SortedMap;
import org.apache.log4j.Logger;

public class User {
    public static Logger LOG = Logger.getLogger(User.class);
    private String id = null;
    private SortedMap<String, Long> buckets = null;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public SortedMap<String, Long> getBuckets() {
        return buckets;
    }

    public void setBuckets(SortedMap<String, Long> buckets) {
        this.buckets = buckets;
    }

    @Override
    public String toString() {
        return "User [id=" + id + "]";
    }

    public User(String id) {
        this.id = id;
    }
}
