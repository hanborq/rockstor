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

package com.rockstor.client;

import java.util.HashMap;

public enum ObjectDefaultAcl {

    PRIVATE, PUBLIC_READ, PUBLIC_READ_WRITE, AUTHENTICATIED_READ;

    private static HashMap<ObjectDefaultAcl, String> map;
    static {
        map = new HashMap<ObjectDefaultAcl, String>();
        map.put(PRIVATE, "private");
        map.put(PUBLIC_READ, "public-read");
        map.put(PUBLIC_READ_WRITE, "public-read-write");
        map.put(AUTHENTICATIED_READ, "authenticatied-read");
    }

    static String getAclString(ObjectDefaultAcl acl) {
        return map.get(acl);
    }

}
