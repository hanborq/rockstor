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

package com.rockstor.monitor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ListenerFactory {
    private static ListenerFactory instance = null;
    private ConcurrentMap<String, Listener> listenerMap = new ConcurrentHashMap<String, Listener>();

    public static ListenerFactory getInstance() {
        if (instance == null) {
            instance = new ListenerFactory();
        }
        return instance;
    }

    public Listener getListener(String name) {
        Listener listener = listenerMap.get(name);
        if (listener == null) {
            listener = new DefaultListener(name);
            listenerMap.put(name, listener);
        }
        return listener;
    }
}
