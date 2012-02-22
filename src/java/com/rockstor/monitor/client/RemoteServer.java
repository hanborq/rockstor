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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class RemoteServer {
    private List<Item> items = new LinkedList<Item>();
    private String addressStr = null;
    private String host = null;
    private int port = 0;

    public RemoteServer() {
    }

    public String getHost() {
        assert (addressStr != null);
        return host;
    }

    public int getPort() {
        assert (addressStr != null);
        return port;
    }

    public String getAddress() {
        assert (addressStr != null);
        return addressStr;
    }

    public void setAddress(String address) {
        assert (address != null && !address.isEmpty());
        int idx = address.indexOf(":");
        host = address.substring(0, idx);
        port = Integer.parseInt(address.substring(idx + 1));

        this.addressStr = address;
    }

    public List<Item> getItems() {
        return items;
    }

    public void setItems(Item[] items) {
        this.items.clear();
        this.items = new LinkedList<Item>(Arrays.asList(items));
    }

    public void addItem(Item item) {
        items.add(item);
    }

    @Override
    public String toString() {
        return String.format("[Server: address=%s]", getAddress());
    }

    public static void main(String[] argv) {
        RemoteServer serv = new RemoteServer();
        serv.setAddress("10.24.1.10:10101");
        System.out.println(serv);
    }
}
